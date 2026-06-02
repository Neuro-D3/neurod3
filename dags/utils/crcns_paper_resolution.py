"""
Resolve CRCNS dataset -> paper DOIs/titles/authors.

CRCNS has no metadata API, so we scrape the dataset's landing page and the
companion "about" page for paper DOIs, then resolve each DOI via the shared
Crossref/OpenAlex helpers used by DANDI and OpenNeuro.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from utils.find_reuse_core import (
    Telemetry,
    extract_dois_from_text,
    normalize_doi,
    resolve_crossref_metadata,
    resolve_openalex_work,
)

logger = logging.getLogger(__name__)


# DOIs that appear on many CRCNS about pages but are not dataset-specific
# primary papers. Mirrors the list in
# _tmp_find_reuse_friend/archives/crcns.py.
CRCNS_INFRASTRUCTURE_DOIS: Set[str] = {
    # Teeters et al. 2008 — "Data Sharing for Computational Neuroscience"
    "10.1007/s12021-008-9009-y",
}


# Maximum number of primary papers to keep per dataset. CRCNS about pages
# sometimes list a dozen references in passing; cap so we don't flood the
# mapping table with peripheral citations.
MAX_PAPERS_PER_DATASET = 3


# Subpaths to try off the dataset landing page when looking for paper DOIs.
# Order matters — landing page first, then "/about" pages with both naming
# conventions seen in the wild.
_ABOUT_SUFFIXES = ("", "/about", "/about-{code}")


_USER_AGENT = "D3-CRCNS-PaperMapping/1.0"


@dataclass
class CrcnsPaperResolutionResult:
    papers: List[Dict[str, Any]] = field(default_factory=list)
    telemetry: Dict[str, Any] = field(default_factory=dict)
    reason: Optional[str] = None
    error: Optional[str] = None


def _candidate_urls(dataset_url: Optional[str], dataset_id: str) -> List[str]:
    """Build the ordered list of URLs to probe for DOIs.

    Always includes the landing page first. The CRCNS URL convention is
    `/data-sets/{category}/{code}` and the optional `/about` subpage either
    has no suffix or has `/about-{code}`.
    """
    urls: List[str] = []
    if isinstance(dataset_url, str) and dataset_url.strip():
        base = dataset_url.rstrip("/")
        # Use the code segment (after the last slash) for the
        # `about-{code}` naming convention.
        last = base.rsplit("/", 1)[-1]
        for suffix in _ABOUT_SUFFIXES:
            urls.append(base + suffix.format(code=last))
    # As a last resort, attempt the canonical pattern from the code itself.
    elif "/" not in dataset_id and "-" in dataset_id:
        # Without a stored URL we can't know the category; the resolver will
        # simply yield no candidates.
        pass
    return urls


def _extract_paper_dois_from_html(html: str) -> List[str]:
    """Pull paper-like DOIs from raw HTML, with CRCNS-specific filtering."""
    if not isinstance(html, str) or not html:
        return []

    raw = extract_dois_from_text(html)

    paper_dois: List[str] = []
    seen: Set[str] = set()
    for doi in raw:
        doi_clean = doi.rstrip(".,;:/")
        norm = normalize_doi(doi_clean)
        if not norm:
            continue
        # CRCNS' own dataset DOIs live under 10.6080 — skip those.
        if norm.startswith("10.6080/"):
            continue
        if norm in CRCNS_INFRASTRUCTURE_DOIS:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        paper_dois.append(norm)
    return paper_dois


def _scrape_doi_candidates(
    session: requests.Session,
    dataset_url: Optional[str],
    dataset_id: str,
    *,
    telemetry: Telemetry,
) -> Tuple[List[str], Optional[str]]:
    """Return (paper_dois, reason). reason is non-None on outright failure."""
    urls = _candidate_urls(dataset_url, dataset_id)
    if not urls:
        return [], "no_landing_url"

    last_error: Optional[str] = None
    for url in urls:
        try:
            resp = session.get(url, timeout=15, headers={"User-Agent": _USER_AGENT})
        except requests.RequestException as e:
            last_error = f"request_error:{e.__class__.__name__}"
            telemetry.api_5xx_count += 1
            continue
        if resp.status_code == 404:
            continue
        if resp.status_code >= 500:
            telemetry.api_5xx_count += 1
            last_error = f"server_error_{resp.status_code}"
            continue
        if resp.status_code != 200:
            last_error = f"http_{resp.status_code}"
            continue

        dois = _extract_paper_dois_from_html(resp.text)
        if dois:
            return dois[:MAX_PAPERS_PER_DATASET], None

    return [], last_error or "no_dois_found"


def resolve_papers_for_crcns_dataset(
    *,
    dataset_id: str,
    dataset_title: Optional[str],
    dataset_description: Optional[str],
    dataset_url: Optional[str],
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> CrcnsPaperResolutionResult:
    """Find primary paper DOIs for a CRCNS dataset and enrich metadata.

    Strategy:
    1. GET the dataset landing page (and `/about` variants) and regex-extract
       DOIs from raw HTML.
    2. As a fallback, look for DOIs in the dataset description text already
       stored in Postgres.
    3. For each remaining DOI, resolve title/authors via Crossref then
       OpenAlex (shared helpers).
    """
    telemetry = Telemetry()
    session = requests.Session()
    session.headers.update({"User-Agent": _USER_AGENT})

    dois, scrape_reason = _scrape_doi_candidates(
        session, dataset_url, dataset_id, telemetry=telemetry,
    )

    if not dois:
        # Fallback: scan the description text we already have in Postgres.
        if isinstance(dataset_description, str) and dataset_description.strip():
            text_dois = [
                d for d in extract_dois_from_text(dataset_description)
                if not d.startswith("10.6080/") and d not in CRCNS_INFRASTRUCTURE_DOIS
            ]
            # Dedup while preserving order.
            seen: Set[str] = set()
            for d in text_dois:
                n = normalize_doi(d)
                if not n or n in seen:
                    continue
                seen.add(n)
                dois.append(n)

    if not dois:
        return CrcnsPaperResolutionResult(
            papers=[],
            telemetry=telemetry.to_dict(),
            reason=scrape_reason or "no_dois_found",
            error=None,
        )

    out: List[Dict[str, Any]] = []
    for doi in dois[:MAX_PAPERS_PER_DATASET]:
        doi_norm = normalize_doi(doi)
        if not doi_norm:
            continue

        paper: Dict[str, Any] = {
            "doi": doi_norm,
            "title": None,
            "openalex_id": None,
            "authors": None,
            "source": "crcns_about_page",
            "relation_type": "linked",
            "paper_metadata_source": None,
            "publication_date": None,
            "publication_year": None,
            "journal": None,
            "senior_author_country": None,
        }

        cr = resolve_crossref_metadata(
            session,
            doi_norm,
            telemetry=telemetry,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        if cr.get("title"):
            paper["title"] = cr.get("title")
            paper["paper_metadata_source"] = "crossref"
        if cr.get("authors"):
            paper["authors"] = cr.get("authors")
            paper["paper_metadata_source"] = paper.get("paper_metadata_source") or "crossref"
        if cr.get("publication_date"):
            paper["publication_date"] = cr.get("publication_date")
        if cr.get("publication_year"):
            paper["publication_year"] = cr.get("publication_year")

        if not paper.get("title") or not paper.get("authors") or not paper.get("openalex_id"):
            oa = resolve_openalex_work(
                session,
                doi_norm,
                telemetry=telemetry,
                min_interval_seconds=min_interval_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            if not paper.get("title") and oa.get("title"):
                paper["title"] = oa.get("title")
            if oa.get("openalex_id"):
                paper["openalex_id"] = oa.get("openalex_id")
            if not paper.get("authors") and oa.get("authors"):
                paper["authors"] = oa.get("authors")
            if not paper.get("publication_date") and oa.get("publication_date"):
                paper["publication_date"] = oa.get("publication_date")
            if not paper.get("publication_year") and oa.get("publication_year"):
                paper["publication_year"] = oa.get("publication_year")
            if (oa.get("title") or oa.get("openalex_id") or oa.get("authors")) and not paper.get("paper_metadata_source"):
                paper["paper_metadata_source"] = "openalex"
            if not paper.get("journal") and oa.get("journal"):
                paper["journal"] = oa["journal"]
            if not paper.get("senior_author_country") and oa.get("senior_author_country"):
                paper["senior_author_country"] = oa["senior_author_country"]

        out.append(paper)

    return CrcnsPaperResolutionResult(
        papers=out,
        telemetry=telemetry.to_dict(),
        reason=None,
        error=None,
    )
