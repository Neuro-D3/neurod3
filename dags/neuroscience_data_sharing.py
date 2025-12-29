"""
Script to fetch neuroscience-related articles from PMC and bioRxiv,
download full-text XML, and analyze for data sharing indicators.

Sources:
- PMC Open Access Subset via NCBI E-utilities API
- bioRxiv via bioRxiv API + S3 TDM repository

Features:
- Local caching of full-text XML to avoid re-downloading
- Data sharing analysis (detects repositories, accession numbers, sharing patterns)

This script is designed to be converted into an Airflow DAG.
"""
import io
import json
import logging
import os
import re
import struct
import time
import zipfile
import zlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import backoff
import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm

# Load .env file from project root
_project_root = Path(__file__).parent.parent
load_dotenv(_project_root / ".env")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# =============================================================================
# Configuration
# =============================================================================

# Cache directories
CACHE_BASE_DIR = _project_root / "data"
BIORXIV_CACHE_DIR = CACHE_BASE_DIR / "biorxiv_xml"
PMC_CACHE_DIR = CACHE_BASE_DIR / "pmc_xml"

# NCBI E-utilities
EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NCBI_REQUEST_DELAY = 0.34  # Seconds between requests without API key
PMC_NEUROSCIENCE_QUERY = (
    '("neuroscience"[MeSH Terms] OR "neurosciences"[MeSH Terms] OR '
    '"brain"[MeSH Terms] OR "neurons"[MeSH Terms] OR '
    '"nervous system"[MeSH Terms]) AND "open access"[filter]'
)

# bioRxiv API
BIORXIV_API_BASE = "https://api.biorxiv.org"

# bioRxiv S3 TDM repository
BIORXIV_S3_BUCKET = "biorxiv-src-monthly"
BIORXIV_S3_REGION = "us-east-1"

# Known data repositories for detection
DATA_REPOSITORIES = [
    # Neuroscience-specific
    "dandi", "dandiarchive", "openneuro", "neurodata", "neuromorpho",
    "allen brain", "allen institute", "brain-map",
    # General scientific
    "zenodo", "figshare", "dryad", "osf", "open science framework",
    "dataverse", "mendeley data", "harvard dataverse",
    # Genomics/omics
    "arrayexpress", "ena", "european nucleotide", "ncbi",
    "genbank", "uniprot", "pride", "proteomexchange",
    # Imaging
    "image data resource", "empiar", "electron microscopy",
    "bioimage archive", "omero",
    # Code repositories (for data/code sharing)
    "github", "gitlab", "bitbucket", "code ocean", "codeocean",
]


def get_env_var(name: str) -> Optional[str]:
    """Get environment variable value."""
    return os.getenv(name)


def get_ncbi_api_key() -> Optional[str]:
    """Get NCBI API key from environment variable."""
    return get_env_var("NCBI_API_KEY")


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=3)
def _http_get_with_retry(url: str, **kwargs) -> requests.Response:
    """Make HTTP GET request with exponential backoff retry."""
    response = requests.get(url, **kwargs)
    response.raise_for_status()
    return response


@backoff.on_exception(backoff.expo, ClientError, max_tries=3)
def _s3_get_with_retry(s3_client, **kwargs):
    """Make S3 get_object request with exponential backoff retry."""
    return s3_client.get_object(**kwargs)


@backoff.on_exception(backoff.expo, ClientError, max_tries=3)
def _s3_head_with_retry(s3_client, **kwargs):
    """Make S3 head_object request with exponential backoff retry."""
    return s3_client.head_object(**kwargs)


# =============================================================================
# Cache Management
# =============================================================================

def ensure_cache_dirs():
    """Create cache directories if they don't exist."""
    BIORXIV_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PMC_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_cache_path(source: str, article_id: str) -> Path:
    """Get the cache file path for an article."""
    # Sanitize the ID for use as filename
    safe_id = article_id.replace("/", "_").replace(":", "_")

    if source == "biorxiv":
        return BIORXIV_CACHE_DIR / f"{safe_id}.xml"
    elif source == "pmc":
        return PMC_CACHE_DIR / f"{safe_id}.xml"
    else:
        raise ValueError(f"Unknown source: {source}")


def is_cached(source: str, article_id: str) -> bool:
    """Check if an article's full text is cached."""
    cache_path = get_cache_path(source, article_id)
    return cache_path.exists() and cache_path.stat().st_size > 0


def load_from_cache(source: str, article_id: str) -> Optional[str]:
    """Load full-text XML from cache."""
    cache_path = get_cache_path(source, article_id)
    if cache_path.exists():
        try:
            return cache_path.read_text(encoding="utf-8")
        except Exception as e:
            logger.warning("Error reading cache file %s: %s", cache_path, e)
    return None


def save_to_cache(source: str, article_id: str, content: str):
    """Save full-text XML to cache."""
    ensure_cache_dirs()
    cache_path = get_cache_path(source, article_id)
    try:
        cache_path.write_text(content, encoding="utf-8")
        logger.debug("Cached %s to %s", article_id, cache_path)
    except Exception as e:
        logger.warning("Error saving to cache %s: %s", cache_path, e)


def get_cache_stats() -> Dict[str, int]:
    """Get statistics about cached articles."""
    ensure_cache_dirs()
    return {
        "biorxiv": len(list(BIORXIV_CACHE_DIR.glob("*.xml"))),
        "pmc": len(list(PMC_CACHE_DIR.glob("*.xml"))),
    }


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class Article:
    """Represents an article with metadata and data sharing analysis."""
    id: str
    title: str
    source: str  # "pmc" or "biorxiv"
    doi: Optional[str] = None
    authors: List[str] = field(default_factory=list)
    abstract: Optional[str] = None
    pub_date: Optional[str] = None
    category: Optional[str] = None
    full_text: Optional[str] = None

    # Data sharing analysis results
    has_data_availability_section: bool = False
    data_availability_text: Optional[str] = None
    detected_repositories: List[str] = field(default_factory=list)
    detected_accessions: List[str] = field(default_factory=list)
    data_sharing_score: float = 0.0
    shares_data: bool = False
    reuses_data: bool = False


@dataclass
class DataSharingAnalysis:
    """Results of data sharing analysis for an article."""
    has_data_section: bool = False
    data_section_text: str = ""
    repositories_mentioned: Set[str] = field(default_factory=set)
    accession_numbers: List[str] = field(default_factory=list)
    shares_new_data: bool = False
    reuses_existing_data: bool = False
    confidence_score: float = 0.0


# =============================================================================
# bioRxiv API Functions
# =============================================================================

def fetch_biorxiv_articles(
    start_date: str,
    end_date: str,
    server: str = "biorxiv",
    category: Optional[str] = "neuroscience",
    max_results: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch article metadata from bioRxiv API.
    """
    articles = []
    cursor = 0
    total_available = None
    pbar = None

    while True:
        url = f"{BIORXIV_API_BASE}/details/{server}/{start_date}/{end_date}/{cursor}/json"

        try:
            response = _http_get_with_retry(url, timeout=60)
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error("Error fetching bioRxiv data after retries: %s", e)
            break

        collection = data.get("collection", [])
        if not collection:
            break

        # Initialize progress bar on first response
        messages = data.get("messages", [{}])
        if pbar is None and messages:
            total_available = int(messages[0].get("total", 0))
            target = min(total_available, max_results) if max_results else total_available
            pbar = tqdm(
                total=target,
                desc=f"Fetching {server} metadata",
                unit="article",
            )

        for item in collection:
            # Filter by category if specified
            if category and item.get("category", "").lower() != category.lower():
                continue

            articles.append({
                "doi": item.get("doi"),
                "title": item.get("title"),
                "authors": item.get("authors", ""),
                "author_corresponding": item.get("author_corresponding"),
                "abstract": item.get("abstract"),
                "category": item.get("category"),
                "date": item.get("date"),
                "version": item.get("version"),
                "type": item.get("type"),
                "license": item.get("license"),
            })

            if pbar:
                pbar.update(1)

            if max_results and len(articles) >= max_results:
                if pbar:
                    pbar.close()
                return articles

        # Check for more pages
        if messages:
            count = messages[0].get("count", 0)
            cursor += count

            if total_available is not None and cursor >= total_available:
                break
        else:
            break

        time.sleep(0.1)

    if pbar:
        pbar.close()

    return articles


def search_biorxiv_neuroscience(
    start_date: str = "2020-01-01",
    end_date: Optional[str] = None,
    max_results: int = 1000,
) -> List[Article]:
    """Search bioRxiv for neuroscience articles."""
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    raw_articles = fetch_biorxiv_articles(
        start_date=start_date,
        end_date=end_date,
        category="neuroscience",
        max_results=max_results,
    )

    articles = []
    for item in raw_articles:
        article = Article(
            id=item.get("doi", ""),
            title=item.get("title", ""),
            source="biorxiv",
            doi=item.get("doi"),
            authors=item.get("authors", "").split("; ") if item.get("authors") else [],
            abstract=item.get("abstract"),
            pub_date=item.get("date"),
            category=item.get("category"),
        )
        articles.append(article)

    return articles


# =============================================================================
# bioRxiv S3 Full-Text Download Functions
# =============================================================================

# Per-month DOI index cache: month -> {doi -> s3_key}
_month_doi_indices: Dict[str, Dict[str, str]] = {}


def get_s3_client():
    """Get boto3 S3 client with credentials from environment."""
    access_key = get_env_var("AWS_ACCESS_KEY_ID")
    secret_key = get_env_var("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "AWS credentials required. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
            "in environment or .env file."
        )

    return boto3.client(
        "s3",
        region_name=BIORXIV_S3_REGION,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def _get_month_index_cache_path(month: str) -> Path:
    """Get cache file path for a month's DOI index."""
    return CACHE_BASE_DIR / f"biorxiv_doi_index_{month}.json"


def _load_month_index_from_cache(month: str) -> Optional[Dict[str, str]]:
    """Load a month's DOI index from disk cache."""
    cache_path = _get_month_index_cache_path(month)
    if not cache_path.exists():
        return None

    # Check age - expire after 7 days (month indices don't change often)
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    age = datetime.now() - mtime
    if age > timedelta(days=7):
        return None

    try:
        with open(cache_path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.debug("Failed to load month index cache for %s: %s", month, e)
        return None


def _save_month_index_to_cache(month: str, index: Dict[str, str]):
    """Save a month's DOI index to disk cache."""
    ensure_cache_dirs()
    cache_path = _get_month_index_cache_path(month)
    try:
        with open(cache_path, "w") as f:
            json.dump(index, f)
    except Exception as e:
        logger.warning("Error saving month index cache: %s", e)


def _extract_doi_from_meca(s3_client, s3_key: str) -> Optional[str]:
    """
    Extract DOI from a MECA file efficiently using range requests.

    Downloads only ~32KB from the end of the file (instead of the full 20MB+ file)
    to extract the DOI from transfer.xml. This reduces data transfer costs by 99.9%+.
    """
    try:
        # Get file size
        head = _s3_head_with_retry(
            s3_client,
            Bucket=BIORXIV_S3_BUCKET,
            Key=s3_key,
            RequestPayer="requester",
        )
        file_size = head["ContentLength"]

        # Download only the last 32KB (contains central directory + transfer.xml)
        # 32KB provides ample margin for large MECA files while still being
        # 99.9%+ more efficient than downloading full 20-400MB files
        tail_size = min(32768, file_size)
        response = _s3_get_with_retry(
            s3_client,
            Bucket=BIORXIV_S3_BUCKET,
            Key=s3_key,
            Range=f"bytes={file_size - tail_size}-{file_size - 1}",
            RequestPayer="requester",
        )
        tail = response["Body"].read()

        # Find End of Central Directory signature
        eocd_pos = tail.rfind(b"PK\x05\x06")
        if eocd_pos == -1:
            return None

        # Parse EOCD to get central directory offset
        cd_offset = struct.unpack("<I", tail[eocd_pos + 16:eocd_pos + 20])[0]
        cd_start_in_file = file_size - tail_size

        if cd_offset < cd_start_in_file:
            return None  # Central directory not in our tail

        # Parse central directory to find transfer.xml
        cd_in_tail = cd_offset - cd_start_in_file
        cd_data = tail[cd_in_tail:]

        pos = 0
        while pos < len(cd_data) - 46:
            if cd_data[pos:pos + 4] != b"PK\x01\x02":
                break

            fname_len = struct.unpack("<H", cd_data[pos + 28:pos + 30])[0]
            extra_len = struct.unpack("<H", cd_data[pos + 30:pos + 32])[0]
            comment_len = struct.unpack("<H", cd_data[pos + 32:pos + 34])[0]
            local_offset = struct.unpack("<I", cd_data[pos + 42:pos + 46])[0]

            fname = cd_data[pos + 46:pos + 46 + fname_len]
            if fname == b"transfer.xml":
                # Found transfer.xml - extract and decompress
                if local_offset >= cd_start_in_file:
                    entry_start = local_offset - cd_start_in_file
                    entry_data = tail[entry_start:]

                    if entry_data[:4] == b"PK\x03\x04":
                        compression = struct.unpack("<H", entry_data[8:10])[0]
                        comp_size = struct.unpack("<I", entry_data[18:22])[0]
                        local_fname_len = struct.unpack("<H", entry_data[26:28])[0]
                        local_extra_len = struct.unpack("<H", entry_data[28:30])[0]

                        data_start = 30 + local_fname_len + local_extra_len
                        compressed = entry_data[data_start:data_start + comp_size]

                        if compression == 8:  # DEFLATE
                            content = zlib.decompress(compressed, -zlib.MAX_WBITS).decode("utf-8")
                        else:  # STORED
                            content = compressed.decode("utf-8")

                        # Extract DOI from reference attribute
                        # Format: /biorxiv/early/2024/12/20/2024.12.18.628357.atom
                        match = re.search(r"/(\d[\d.]+)\.atom", content)
                        if match:
                            return f"10.1101/{match.group(1)}"
                break

            pos += 46 + fname_len + extra_len + comment_len

        return None
    except Exception as e:
        logger.debug("Failed to extract DOI from %s: %s", s3_key, e)
        return None


def _date_to_month_folder(date_str: str) -> Optional[str]:
    """Convert a date string (YYYY-MM-DD) to the S3 month folder name.

    Args:
        date_str: Date in YYYY-MM-DD format (e.g., "2024-12-18")

    Returns:
        Month folder name (e.g., "December_2024")
    """
    match = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
    if not match:
        return None

    year, month_num, _ = match.groups()
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    month_name = months[int(month_num) - 1]
    return f"{month_name}_{year}"


def build_month_doi_index(s3_client, month: str, max_files: int = 10000) -> Dict[str, str]:
    """
    Build a DOI -> S3 key index for a specific month.

    Uses efficient range requests to extract DOIs (~32KB per file instead of ~20MB),
    reducing indexing cost from ~$9/month to ~$0.01/month (99.9%+ savings).

    Args:
        s3_client: Boto3 S3 client
        month: Month folder name (e.g., "December_2024")
        max_files: Maximum number of files to index (default 10000, covers most months)

    Returns:
        Dict mapping DOI -> S3 key
    """
    global _month_doi_indices

    # Check in-memory cache
    if month in _month_doi_indices:
        return _month_doi_indices[month]

    # Check disk cache
    cached = _load_month_index_from_cache(month)
    if cached:
        _month_doi_indices[month] = cached
        logger.info("Loaded %s DOI index from cache (%d DOIs)", month, len(cached))
        return cached

    # Build index by downloading MECA files
    index = {}
    prefix = f"Current_Content/{month}/"

    logger.info("Building DOI index for %s (this may take a while)...", month)

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        s3_keys = []

        for page in paginator.paginate(
            Bucket=BIORXIV_S3_BUCKET,
            Prefix=prefix,
            RequestPayer="requester",
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".meca"):
                    s3_keys.append(obj["Key"])

        # Limit for cost control
        if len(s3_keys) > max_files:
            logger.info("Limiting index to %d of %d files in %s", max_files, len(s3_keys), month)
            s3_keys = s3_keys[:max_files]

        # Extract DOIs in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_key = {
                executor.submit(_extract_doi_from_meca, s3_client, key): key
                for key in s3_keys
            }

            with tqdm(total=len(s3_keys), desc=f"Indexing {month}", unit="file") as pbar:
                for future in as_completed(future_to_key):
                    s3_key = future_to_key[future]
                    try:
                        doi = future.result()
                        if doi:
                            index[doi] = s3_key
                    except Exception as e:
                        logger.debug("Failed to process %s: %s", s3_key, e)
                    pbar.update(1)

        logger.info("Built %s DOI index: %d DOIs", month, len(index))

        # Save to caches
        _month_doi_indices[month] = index
        _save_month_index_to_cache(month, index)

        return index

    except Exception as e:
        logger.error("Error building DOI index for %s: %s", month, e)
        return {}


def find_meca_file_for_doi(s3_client, doi: str, pub_date: Optional[str] = None) -> Optional[str]:
    """Find the S3 key for a DOI by looking in the appropriate month's index.

    Args:
        s3_client: Boto3 S3 client
        doi: The article DOI
        pub_date: Publication date in YYYY-MM-DD format (used to determine month folder)
    """
    # Determine which month folder to look in using publication date
    if not pub_date:
        logger.debug("No publication date for DOI: %s", doi)
        return None

    month = _date_to_month_folder(pub_date)
    if not month:
        logger.debug("Cannot determine month folder from date %s for DOI: %s", pub_date, doi)
        return None

    # Build/load the month index
    index = build_month_doi_index(s3_client, month)

    return index.get(doi)


def download_and_extract_xml_from_meca(s3_client, s3_key: str) -> Optional[str]:
    """Download a .meca file from S3 and extract the full-text XML content."""
    try:
        response = _s3_get_with_retry(
            s3_client,
            Bucket=BIORXIV_S3_BUCKET,
            Key=s3_key,
            RequestPayer="requester",
        )

        meca_content = response["Body"].read()

        with zipfile.ZipFile(io.BytesIO(meca_content)) as zf:
            for filename in zf.namelist():
                if filename.endswith(".xml") and "content" in filename.lower():
                    with zf.open(filename) as xml_file:
                        return xml_file.read().decode("utf-8")

        logger.warning("No XML file found in MECA archive: %s", s3_key)
        return None

    except Exception as e:
        logger.error("Error extracting XML from MECA file %s: %s", s3_key, e)
        return None


def fetch_biorxiv_full_text(
    article: Article,
    s3_client,
    use_cache: bool = True,
) -> Optional[str]:
    """Fetch full-text XML for a bioRxiv article from S3."""
    if not article.doi:
        logger.warning("No DOI for article: %s", article.title[:50] if article.title else "Unknown")
        return None

    # Check cache first
    if use_cache and is_cached("biorxiv", article.doi):
        logger.debug("Loading from cache: %s", article.doi)
        return load_from_cache("biorxiv", article.doi)

    # Find the MECA file in S3 using the article's publication date
    s3_key = find_meca_file_for_doi(s3_client, article.doi, article.pub_date)
    if not s3_key:
        logger.debug("No MECA file found for DOI: %s", article.doi)
        return None

    # Download and extract XML
    xml_content = download_and_extract_xml_from_meca(s3_client, s3_key)

    if xml_content and use_cache:
        save_to_cache("biorxiv", article.doi, xml_content)

    return xml_content


# =============================================================================
# PMC Functions
# =============================================================================

def search_pmc_neuroscience_articles(
    max_results: int = 10000,
    api_key: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Search PMC for neuroscience-related open access articles."""
    search_query = PMC_NEUROSCIENCE_QUERY

    # Add date filter if specified
    if start_date:
        search_query = f'{search_query} AND ("{start_date}"[PDAT] : "{end_date or "3000"}"[PDAT])'

    esearch_url = f"{EUTILS_BASE}/esearch.fcgi"
    params = {
        "db": "pmc",
        "term": search_query,
        "retmax": max_results,
        "retmode": "json",
        "usehistory": "y",
    }

    if api_key:
        params["api_key"] = api_key

    try:
        logger.info("Searching PMC for neuroscience articles (max_results=%d)", max_results)
        response = _http_get_with_retry(esearch_url, params=params, timeout=60)

        data = response.json()
        esearch_result = data.get("esearchresult", {})
        count = int(esearch_result.get("count", 0))
        id_list = esearch_result.get("idlist", [])

        logger.info(
            "Found %d total neuroscience articles in PMC OA, retrieved %d IDs",
            count, len(id_list),
        )
        return id_list

    except requests.exceptions.RequestException as e:
        logger.error("Error searching PMC: %s", e)
        raise


def fetch_pmc_article_metadata(
    pmc_ids: List[str],
    api_key: Optional[str] = None,
) -> List[Article]:
    """Fetch article metadata for PMC IDs using E-summary."""
    if not pmc_ids:
        return []

    esummary_url = f"{EUTILS_BASE}/esummary.fcgi"
    articles = []

    # Process in batches of 200
    batch_size = 200
    for i in range(0, len(pmc_ids), batch_size):
        batch = pmc_ids[i:i + batch_size]

        params = {
            "db": "pmc",
            "id": ",".join(batch),
            "retmode": "json",
        }
        if api_key:
            params["api_key"] = api_key

        try:
            response = _http_get_with_retry(esummary_url, params=params, timeout=60)
            data = response.json()
            result = data.get("result", {})

            for pmc_id in batch:
                article_data = result.get(pmc_id, {})
                if article_data and "error" not in article_data:
                    # Extract DOI from articleids
                    doi = None
                    for aid in article_data.get("articleids", []):
                        if aid.get("idtype") == "doi":
                            doi = aid.get("value")
                            break

                    article = Article(
                        id=f"PMC{pmc_id}",
                        title=article_data.get("title", ""),
                        source="pmc",
                        doi=doi,
                        pub_date=article_data.get("pubdate", ""),
                        authors=[a.get("name", "") for a in article_data.get("authors", [])],
                    )
                    articles.append(article)

            if not api_key:
                time.sleep(NCBI_REQUEST_DELAY)

        except requests.exceptions.RequestException as e:
            logger.error("Error fetching PMC metadata: %s", e)

        if (i + batch_size) % 1000 == 0:
            logger.info("Fetched metadata for %d/%d PMC articles", i + batch_size, len(pmc_ids))

    return articles


def fetch_pmc_full_text(
    article: Article,
    api_key: Optional[str] = None,
    use_cache: bool = True,
) -> Optional[str]:
    """
    Fetch full-text XML for a PMC article using efetch.
    """
    pmc_id = article.id.replace("PMC", "")

    # Check cache first
    if use_cache and is_cached("pmc", article.id):
        logger.debug("Loading from cache: %s", article.id)
        return load_from_cache("pmc", article.id)

    # Fetch from NCBI
    efetch_url = f"{EUTILS_BASE}/efetch.fcgi"
    params = {
        "db": "pmc",
        "id": pmc_id,
        "rettype": "xml",
        "retmode": "xml",
    }
    if api_key:
        params["api_key"] = api_key

    try:
        response = _http_get_with_retry(efetch_url, params=params, timeout=60)
        xml_content = response.text

        # Verify we got valid XML
        if xml_content and ("<pmc-articleset" in xml_content or "<article" in xml_content):
            if use_cache:
                save_to_cache("pmc", article.id, xml_content)
            return xml_content
        else:
            logger.debug("No valid XML returned for %s", article.id)
            return None

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching PMC full text for %s: %s", article.id, e)
        return None


def get_pmc_neuroscience_articles(
    start_date: str = "2024-01-01",
    end_date: Optional[str] = None,
    max_results: int = 1000,
    api_key: Optional[str] = None,
) -> List[Article]:
    """Get neuroscience articles from PMC."""
    # Search for article IDs
    pmc_ids = search_pmc_neuroscience_articles(
        max_results=max_results,
        api_key=api_key,
        start_date=start_date,
        end_date=end_date,
    )

    if not pmc_ids:
        return []

    # Fetch metadata
    articles = fetch_pmc_article_metadata(pmc_ids, api_key=api_key)

    return articles


# =============================================================================
# Data Sharing Analysis Functions
# =============================================================================

def extract_text_from_xml(xml_content: str) -> str:
    """Extract plain text from JATS XML content."""
    try:
        root = ET.fromstring(xml_content)
        text_parts = []
        for elem in root.iter():
            if elem.text:
                text_parts.append(elem.text)
            if elem.tail:
                text_parts.append(elem.tail)
        return " ".join(text_parts)
    except ET.ParseError as e:
        logger.error("Error parsing XML: %s", e)
        return ""


def extract_data_availability_section(xml_content: str) -> Optional[str]:
    """Extract the Data Availability section from JATS XML."""
    try:
        root = ET.fromstring(xml_content)

        data_section_patterns = [
            r"data\s*availab",
            r"data\s*access",
            r"code\s*availab",
            r"data\s*and\s*code",
            r"data\s*sharing",
            r"availability\s*of\s*data",
            r"supporting\s*information",
            r"data\s*deposition",
        ]

        for sec in root.iter("sec"):
            title_elem = sec.find("title")
            if title_elem is not None and title_elem.text:
                title_lower = title_elem.text.lower()
                for pattern in data_section_patterns:
                    if re.search(pattern, title_lower):
                        text_parts = []
                        for elem in sec.iter():
                            if elem.text:
                                text_parts.append(elem.text)
                            if elem.tail:
                                text_parts.append(elem.tail)
                        return " ".join(text_parts)

        for notes in root.iter("notes"):
            notes_type = notes.get("notes-type", "")
            if "data" in notes_type.lower() or "availability" in notes_type.lower():
                text_parts = []
                for elem in notes.iter():
                    if elem.text:
                        text_parts.append(elem.text)
                    if elem.tail:
                        text_parts.append(elem.tail)
                return " ".join(text_parts)

        return None
    except ET.ParseError:
        return None


def detect_repositories(text: str) -> Set[str]:
    """Detect mentions of known data repositories in text."""
    text_lower = text.lower()
    found = set()
    for repo in DATA_REPOSITORIES:
        if repo.lower() in text_lower:
            found.add(repo)
    return found


def extract_accession_numbers(text: str) -> List[str]:
    """Extract accession numbers and identifiers from text."""
    accessions = []

    # DOIs (often used for datasets)
    accessions.extend(re.findall(r"10\.\d{4,}/[^\s,;)]+", text))

    # DANDI identifiers
    accessions.extend(re.findall(r"DANDI:\d+", text, re.IGNORECASE))
    accessions.extend(re.findall(r"dandiset/\d+", text, re.IGNORECASE))

    # OpenNeuro identifiers
    accessions.extend(re.findall(r"ds\d{6}", text, re.IGNORECASE))

    return list(set(accessions))


def analyze_data_sharing(article: Article) -> DataSharingAnalysis:
    """Analyze an article for data sharing indicators."""
    analysis = DataSharingAnalysis()

    if not article.full_text:
        return analysis

    data_section = extract_data_availability_section(article.full_text)
    if data_section:
        analysis.has_data_section = True
        analysis.data_section_text = data_section

    full_text = extract_text_from_xml(article.full_text)
    analysis_text = data_section if data_section else full_text

    analysis.repositories_mentioned = detect_repositories(analysis_text)
    analysis.accession_numbers = extract_accession_numbers(analysis_text)

    text_lower = analysis_text.lower()

    shares_patterns = [
        r"we\s+(?:have\s+)?deposited",
        r"data\s+(?:are|is|were|has been)\s+deposited",
        r"(?:are|is)\s+available\s+(?:at|from|in|on)",
        r"we\s+(?:have\s+)?(?:made|uploaded|shared)",
        r"can\s+be\s+(?:accessed|downloaded|found)\s+(?:at|from)",
    ]

    reuses_patterns = [
        r"(?:we\s+)?(?:used|analyzed|obtained|downloaded)\s+(?:data|datasets?)\s+from",
        r"data\s+(?:were|was)\s+(?:obtained|downloaded|retrieved)\s+from",
        r"publicly\s+available\s+(?:data|datasets?)",
        r"previously\s+published",
        r"existing\s+(?:data|datasets?)",
    ]

    for pattern in shares_patterns:
        if re.search(pattern, text_lower):
            analysis.shares_new_data = True
            break

    for pattern in reuses_patterns:
        if re.search(pattern, text_lower):
            analysis.reuses_existing_data = True
            break

    score = 0.0
    if analysis.has_data_section:
        score += 0.3
    if analysis.repositories_mentioned:
        score += 0.2 * min(len(analysis.repositories_mentioned), 3) / 3
    if analysis.accession_numbers:
        score += 0.3 * min(len(analysis.accession_numbers), 5) / 5
    if analysis.shares_new_data or analysis.reuses_existing_data:
        score += 0.2

    analysis.confidence_score = min(score, 1.0)

    return analysis


# =============================================================================
# Main Pipeline Functions
# =============================================================================

def process_articles(
    articles: List[Article],
    analyze_sharing: bool = True,
    use_cache: bool = True,
    api_key: Optional[str] = None,
) -> List[Article]:
    """
    Process articles: download full text and analyze for data sharing.
    Only returns articles where full text was successfully obtained.
    """
    if not articles:
        return articles

    ensure_cache_dirs()

    # Separate by source
    biorxiv_articles = [a for a in articles if a.source == "biorxiv"]
    pmc_articles = [a for a in articles if a.source == "pmc"]

    # Process bioRxiv articles
    if biorxiv_articles:
        # Check which need downloading
        to_download = [a for a in biorxiv_articles if not (use_cache and is_cached("biorxiv", a.doi or ""))]
        cached = len(biorxiv_articles) - len(to_download)

        if cached > 0:
            logger.info("Loading %d bioRxiv articles from cache", cached)
            for article in biorxiv_articles:
                if use_cache and article.doi and is_cached("biorxiv", article.doi):
                    article.full_text = load_from_cache("biorxiv", article.doi)

        if to_download:
            try:
                s3_client = get_s3_client()

                # Group articles by month to efficiently build indices
                articles_by_month: Dict[str, List[Article]] = {}
                for article in to_download:
                    if article.doi and article.pub_date:
                        month = _date_to_month_folder(article.pub_date)
                        if month:
                            if month not in articles_by_month:
                                articles_by_month[month] = []
                            articles_by_month[month].append(article)

                # Process each month's articles
                for month, month_articles in articles_by_month.items():
                    # Build the index for this month (will cache for reuse)
                    build_month_doi_index(s3_client, month)

                # Now fetch all articles (indices are cached)
                with tqdm(
                    total=len(to_download),
                    desc="Downloading bioRxiv",
                    unit="article",
                ) as pbar:
                    for article in to_download:
                        try:
                            xml_content = fetch_biorxiv_full_text(article, s3_client, use_cache)
                            if xml_content:
                                article.full_text = xml_content
                                pbar.set_postfix({"last": article.doi[:30] + "..." if article.doi else "?"})
                        except Exception as e:
                            logger.error("Error fetching full text for %s: %s", article.doi, e)
                        pbar.update(1)

            except ValueError as e:
                logger.warning("Skipping bioRxiv full-text download: %s", e)

    # Process PMC articles
    if pmc_articles:
        to_download = [a for a in pmc_articles if not (use_cache and is_cached("pmc", a.id))]
        cached = len(pmc_articles) - len(to_download)

        if cached > 0:
            logger.info("Loading %d PMC articles from cache", cached)
            for article in pmc_articles:
                if use_cache and is_cached("pmc", article.id):
                    article.full_text = load_from_cache("pmc", article.id)

        if to_download:
            with tqdm(
                total=len(to_download),
                desc="Downloading PMC",
                unit="article",
            ) as pbar:
                for article in to_download:
                    xml_content = fetch_pmc_full_text(article, api_key=api_key, use_cache=use_cache)
                    if xml_content:
                        article.full_text = xml_content
                        pbar.set_postfix({"last": article.id})

                    if not api_key:
                        time.sleep(NCBI_REQUEST_DELAY)

                    pbar.update(1)

    # Filter to only articles with full text
    articles_with_text = [a for a in articles if a.full_text]
    filtered_count = len(articles) - len(articles_with_text)
    if filtered_count > 0:
        logger.info("Filtered out %d articles without full text", filtered_count)

    if analyze_sharing:
        logger.info("Analyzing data sharing for %d articles...", len(articles_with_text))

        for article in articles_with_text:
            analysis = analyze_data_sharing(article)

            article.has_data_availability_section = analysis.has_data_section
            article.data_availability_text = analysis.data_section_text
            article.detected_repositories = list(analysis.repositories_mentioned)
            article.detected_accessions = analysis.accession_numbers
            article.data_sharing_score = analysis.confidence_score
            article.shares_data = analysis.shares_new_data
            article.reuses_data = analysis.reuses_existing_data

    return articles_with_text


def get_neuroscience_articles_with_data_analysis(
    source: str = "both",
    start_date: str = "2024-01-01",
    end_date: Optional[str] = None,
    target_articles: int = 100,
    use_cache: bool = True,
    api_key: Optional[str] = None,
) -> List[Article]:
    """
    Main function to get neuroscience articles with data sharing analysis.

    Args:
        source: "biorxiv", "pmc", or "both"
        start_date: Start date in YYYY-MM-DD format
        end_date: End date (defaults to today)
        target_articles: Target number of articles WITH full text per source
        use_cache: Whether to use local cache
        api_key: NCBI API key for PMC

    Returns:
        List of Article objects with full text and data sharing analysis.
        Only articles where full text was successfully obtained are returned.
    """
    result_articles = []

    # Fetch more than needed since some won't have full text available
    # Start with 2x overfetch, increase if needed
    overfetch_factor = 2

    if source in ["biorxiv", "both"]:
        logger.info("Fetching bioRxiv articles (target: %d with full text)...", target_articles)
        biorxiv_result = []
        fetch_count = target_articles * overfetch_factor
        max_attempts = 5

        for _ in range(max_attempts):
            biorxiv_articles = search_biorxiv_neuroscience(
                start_date=start_date,
                end_date=end_date,
                max_results=fetch_count,
            )
            logger.info("Fetched %d bioRxiv article metadata", len(biorxiv_articles))

            # Process to get full text
            processed = process_articles(
                biorxiv_articles,
                analyze_sharing=True,
                use_cache=use_cache,
                api_key=api_key,
            )
            biorxiv_result = processed

            if len(biorxiv_result) >= target_articles:
                biorxiv_result = biorxiv_result[:target_articles]
                break
            elif len(biorxiv_articles) < fetch_count:
                # No more articles available
                logger.info("Only %d bioRxiv articles with full text available", len(biorxiv_result))
                break
            else:
                # Need to fetch more
                fetch_count = int(fetch_count * 1.5)
                logger.info("Only got %d with full text, fetching more...", len(biorxiv_result))

        result_articles.extend(biorxiv_result)
        logger.info("Got %d bioRxiv articles with full text", len(biorxiv_result))

    if source in ["pmc", "both"]:
        logger.info("Fetching PMC articles (target: %d with full text)...", target_articles)
        pmc_result = []
        fetch_count = target_articles * overfetch_factor
        max_attempts = 5

        for _ in range(max_attempts):
            pmc_articles = get_pmc_neuroscience_articles(
                start_date=start_date,
                end_date=end_date,
                max_results=fetch_count,
                api_key=api_key,
            )
            logger.info("Fetched %d PMC article metadata", len(pmc_articles))

            # Process to get full text
            processed = process_articles(
                pmc_articles,
                analyze_sharing=True,
                use_cache=use_cache,
                api_key=api_key,
            )
            pmc_result = processed

            if len(pmc_result) >= target_articles:
                pmc_result = pmc_result[:target_articles]
                break
            elif len(pmc_articles) < fetch_count:
                # No more articles available
                logger.info("Only %d PMC articles with full text available", len(pmc_result))
                break
            else:
                # Need to fetch more
                fetch_count = int(fetch_count * 1.5)
                logger.info("Only got %d with full text, fetching more...", len(pmc_result))

        result_articles.extend(pmc_result)
        logger.info("Got %d PMC articles with full text", len(pmc_result))

    return result_articles


# =============================================================================
# CLI Interface
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch neuroscience articles and analyze data sharing"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["biorxiv", "pmc", "both"],
        default="both",
        help="Source to fetch from (default: both)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2024-01-01",
        help="Start date in YYYY-MM-DD format (default: 2024-01-01)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--max-articles",
        type=int,
        default=10,
        help="Target number of articles WITH full text per source (default: 10)",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Ignore cache and re-download all full-text",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--show-cache-stats",
        action="store_true",
        help="Show cache statistics and exit",
    )

    args = parser.parse_args()

    # Show cache stats if requested
    if args.show_cache_stats:
        ensure_cache_dirs()
        stats = get_cache_stats()
        print(f"\nCache Statistics:")
        print(f"  bioRxiv articles cached: {stats['biorxiv']}")
        print(f"  PMC articles cached: {stats['pmc']}")
        print(f"\nCache locations:")
        print(f"  bioRxiv: {BIORXIV_CACHE_DIR}")
        print(f"  PMC: {PMC_CACHE_DIR}")
        exit(0)

    # Get API key
    api_key = get_ncbi_api_key()
    if api_key:
        logger.info("Using NCBI API key")

    # Fetch and analyze articles
    articles = get_neuroscience_articles_with_data_analysis(
        source=args.source,
        start_date=args.start_date,
        end_date=args.end_date,
        target_articles=args.max_articles,
        use_cache=not args.refresh_cache,
        api_key=api_key,
    )

    # Print summary
    print(f"\n{'='*70}")
    print(f"Found {len(articles)} neuroscience articles")
    print('='*70)

    biorxiv_count = sum(1 for a in articles if a.source == "biorxiv")
    pmc_count = sum(1 for a in articles if a.source == "pmc")
    print(f"\nSources:")
    print(f"  - bioRxiv: {biorxiv_count}")
    print(f"  - PMC: {pmc_count}")

    shares_count = sum(1 for a in articles if a.shares_data)
    reuses_count = sum(1 for a in articles if a.reuses_data)
    has_section_count = sum(1 for a in articles if a.has_data_availability_section)

    print(f"\nData Sharing Summary:")
    print(f"  - Articles with Data Availability section: {has_section_count}")
    print(f"  - Articles that share new data: {shares_count}")
    print(f"  - Articles that reuse existing data: {reuses_count}")

    # Show cache stats
    stats = get_cache_stats()
    print(f"\nCache Stats:")
    print(f"  - bioRxiv cached: {stats['biorxiv']}")
    print(f"  - PMC cached: {stats['pmc']}")

    print(f"\nSample articles:")
    for i, article in enumerate(articles[:5], 1):
        title_display = article.title[:60] + "..." if len(article.title) > 60 else article.title
        print(f"\n{i}. [{article.source.upper()}] {title_display}")
        print(f"   ID: {article.id}")
        print(f"   Shares data: {article.shares_data}, Reuses data: {article.reuses_data}")
        if article.detected_repositories:
            print(f"   Repositories: {', '.join(article.detected_repositories)}")
        if article.detected_accessions:
            print(f"   Accessions: {', '.join(article.detected_accessions[:5])}")

    # Save to JSON if requested
    if args.output:
        output_data = []
        for article in articles:
            output_data.append({
                "id": article.id,
                "title": article.title,
                "doi": article.doi,
                "source": article.source,
                "pub_date": article.pub_date,
                "category": article.category,
                "has_data_availability_section": article.has_data_availability_section,
                "shares_data": article.shares_data,
                "reuses_data": article.reuses_data,
                "detected_repositories": article.detected_repositories,
                "detected_accessions": article.detected_accessions,
                "data_sharing_score": article.data_sharing_score,
            })

        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to {args.output}")
