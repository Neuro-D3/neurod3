"""
OA-only full text fetching (Option B), plus paper-text-fetcher helpers.

Sources:
- Europe PMC REST API (best effort)
- NCBI E-utilities for PMC (best effort)

TODO (Option C add-on): cache PDF/HTML payloads and extract offline when we have more compute.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional, Tuple
from urllib.parse import quote
import xml.etree.ElementTree as ET

import json
import requests

from utils.find_reuse_core import Telemetry, normalize_doi, strip_nul

logger = logging.getLogger(__name__)


EUROPE_PMC_BASE = "https://www.ebi.ac.uk/europepmc/webservices/rest"
NCBI_EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


def _strip_xml_to_text(xml_text: str) -> Optional[str]:
    """
    Best-effort conversion of XML to plain text.
    """
    if not isinstance(xml_text, str) or not xml_text.strip():
        return None
    try:
        root = ET.fromstring(xml_text)
        parts = []
        for t in root.itertext():
            if t:
                parts.append(t)
        text = " ".join(parts)
        text = re.sub(r"\\s+", " ", text).strip()
        return text if text else None
    except Exception:
        # Fallback: strip tags very roughly
        text = re.sub(r"<[^>]+>", " ", xml_text)
        text = re.sub(r"\\s+", " ", text).strip()
        return text if text else None


def _get_text_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Optional[str]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        if min_interval_seconds > 0:
            time.sleep(min_interval_seconds)
        telemetry.total_requests += 1
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code in (429, 502, 503, 504):
                if resp.status_code == 429:
                    telemetry.api_429_count += 1
                else:
                    telemetry.api_5xx_count += 1
                telemetry.api_retry_count += 1
                wait = min(backoff_seconds * (2 ** (attempt - 1)), 60.0)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            telemetry.api_retry_count += 1
            wait = min(backoff_seconds * (2 ** (attempt - 1)), 60.0)
            time.sleep(wait)
            continue
        except requests.RequestException as e:
            last_exc = e
            break
    if last_exc:
        logger.debug("Fulltext fetch failed url=%s err=%s", url, last_exc)
    return None


def _europe_pmc_find_pmcid(session: requests.Session, doi: str) -> Optional[str]:
    q = quote(f"DOI:{doi}")
    url = f"{EUROPE_PMC_BASE}/search?query={q}&format=json&pageSize=1&resultType=core"
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        results = (data.get("resultList") or {}).get("result") or []
        if isinstance(results, list) and results:
            pmcid = results[0].get("pmcid")
            if isinstance(pmcid, str) and pmcid.startswith("PMC"):
                return pmcid
    except Exception:
        return None
    return None


def fetch_fulltext_oa(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> Tuple[Optional[str], str, bool, str]:
    """
    Returns: (full_text, source, available, reason)

    Delegates to paper-text-fetcher when it is installed (see the helpers at
    the bottom of this module). ``available`` is True only when the article
    BODY was retrieved; ``reason`` is ``"ok"`` then, and otherwise carries the
    fetcher's three-way status as a prefix (``"metadata_only: ..."`` or
    ``"unavailable: ..."``) so callers that store it, such as the paper-mapping
    DAGs' ``papers.fulltext_reason``, keep the distinction. Without the package
    the original Europe PMC / NCBI PMC path below runs unchanged.
    """
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return None, "none", False, "invalid_doi"

    if get_paper_fetcher() is not None:
        detailed = fetch_fulltext_detailed(doi_norm, telemetry=telemetry)
        if detailed["reason"] != "paper_text_fetcher_not_installed":
            if min_interval_seconds > 0 and not detailed["from_cache"]:
                time.sleep(min_interval_seconds)
            source = detailed["source"] or "none"
            if detailed["status"] == TEXT_STATUS_FULL and detailed["text"]:
                return strip_nul(detailed["text"]), source, True, "ok"
            return None, source, False, f"{detailed['status']}: {detailed.get('reason') or 'no text'}"

    # ---- legacy OA-only path (Europe PMC, then NCBI PMC) -------------------

    # 1) Europe PMC -> fullTextXML (requires PMCID)
    pmcid = _europe_pmc_find_pmcid(session, doi_norm)
    if pmcid:
        url = f"{EUROPE_PMC_BASE}/{pmcid}/fullTextXML"
        xml_text = _get_text_with_retries(
            session,
            url,
            timeout=60,
            telemetry=telemetry,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        txt = _strip_xml_to_text(xml_text) if xml_text else None
        if txt:
            return txt, "europe_pmc", True, "ok"

    # 2) NCBI E-utilities -> search PMC by DOI, then efetch xml
    term = quote(f"{doi_norm}[DOI]")
    esearch_url = f"{NCBI_EUTILS_BASE}/esearch.fcgi?db=pmc&term={term}&retmode=json&retmax=1"
    try:
        # Minimal retry using the same helper (text->json parse)
        raw = _get_text_with_retries(
            session,
            esearch_url,
            timeout=30,
            telemetry=telemetry,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        if raw:
            data = json.loads(raw)
            ids = (((data or {}).get("esearchresult") or {}).get("idlist") or [])
            if isinstance(ids, list) and ids:
                pmc_numeric = ids[0]
                if isinstance(pmc_numeric, str) and pmc_numeric.strip():
                    efetch_url = f"{NCBI_EUTILS_BASE}/efetch.fcgi?db=pmc&id={quote(pmc_numeric)}&retmode=xml"
                    xml_text = _get_text_with_retries(
                        session,
                        efetch_url,
                        timeout=90,
                        telemetry=telemetry,
                        min_interval_seconds=min_interval_seconds,
                        max_retries=max_retries,
                        backoff_seconds=backoff_seconds,
                    )
                    txt = _strip_xml_to_text(xml_text) if xml_text else None
                    if txt:
                        return txt, "pmc", True, "ok"
    except json.JSONDecodeError as e:
        logger.debug("PMC esearch JSON decode failed doi=%s err=%s", doi_norm, e)
    except Exception as e:
        # Best-effort OA lookup: surface details in debug logs, but don't fail the whole resolution.
        logger.debug("PMC eutils lookup failed doi=%s err=%s", doi_norm, e, exc_info=True)

    return None, "none", False, "no_oa_fulltext_found"



# ---------------------------------------------------------------------------
# paper-text-fetcher integration
# ---------------------------------------------------------------------------
#
# The upstream find_reuse project moved DOI -> full-text retrieval into a
# standalone package, ``paper-text-fetcher`` (catalystneuro/paper-text-fetcher).
# It tries Europe PMC, NCBI PMC, CrossRef, Elsevier, Unpaywall PDFs, publisher
# HTML and a headless browser, and it reports a three-way status so a title +
# abstract is never mistaken for a paper body:
#
#     full_text      the article body was retrieved (>= MIN_FULL_TEXT_CHARS)
#     metadata_only  title/abstract/references only (closed access)
#     unavailable    nothing came back
#
# ``fetch_fulltext_oa`` above keeps its Europe PMC / NCBI-only implementation
# for now; the helpers below are additive and are what the classification DAG
# uses. The package is optional at import time so DAG parsing never depends on
# it having been installed into the image.

import os
import threading
from pathlib import Path
from typing import Any, Dict, List

FETCHER_TOOL_NAME = "neurod3"
# DOI prefixes routed to the fetcher's bioRxiv/medRxiv Chromium path.
# 10.1101 is the historical bioRxiv/medRxiv prefix; 10.64898 is bioRxiv's
# prefix for deposits from 2026 on (see D3PaperFetcher in get_paper_fetcher).
PREPRINT_DOI_PREFIXES = ("10.1101/", "10.64898/")
TEXT_STATUS_FULL = "full_text"
TEXT_STATUS_METADATA = "metadata_only"
TEXT_STATUS_UNAVAILABLE = "unavailable"

# (env var, default directory under <airflow home>/output/) for each paper-mapping
# DAG's ``_get_output_root``. ``papers.fulltext_cache_key`` is relative to
# whichever of these wrote it, and ``papers.source`` does not say which, so the
# loader probes them in order.
MAPPING_OUTPUT_ROOTS = (
    ("DANDI_PAPER_MAPPING_OUTPUT_DIR", "dandi_paper_mapping"),
    ("OPENNEURO_PAPER_MAPPING_OUTPUT_DIR", "openneuro_paper_mapping"),
    ("CRCNS_PAPER_MAPPING_OUTPUT_DIR", "crcns_paper_mapping"),
    ("SPARC_PAPER_MAPPING_OUTPUT_DIR", "sparc_paper_mapping"),
)

_fetcher_local = threading.local()
_fetcher_import_warned = False


def _dags_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def _airflow_home() -> Path:
    """Parent of the DAG folder: /opt/airflow in the containers, airflow/ in the repo."""
    return _dags_dir().parent


def fetcher_cache_dir() -> Path:
    """Where paper-text-fetcher keeps its JSON-per-DOI cache."""
    env = os.getenv("PAPER_FETCHER_CACHE_DIR", "").strip()
    if env:
        return Path(env)
    return _airflow_home() / "output" / "paper_text_fetcher"


def mapping_output_roots() -> List[Path]:
    """The four paper-mapping DAGs' output roots, in probe order."""
    roots: List[Path] = []
    for env_name, default_dir in MAPPING_OUTPUT_ROOTS:
        env = os.getenv(env_name, "").strip()
        roots.append(Path(env) if env else _airflow_home() / "output" / default_dir)
    return roots


def _fetcher_api_keys() -> Dict[str, str]:
    key = (os.getenv("ELSEVIER_API_KEY") or os.getenv("SCOPUS_API_KEY") or "").strip()
    return {"elsevier": key} if key else {}


def get_paper_fetcher(cache_dir: Optional[Path] = None):
    """
    Return this thread's ``PaperFetcher``, or None if the package is missing.

    One instance per thread: the fetcher holds a requests.Session and, with the
    browser extra, a Playwright context, neither of which is thread-safe.
    """
    global _fetcher_import_warned
    try:
        from paper_text_fetcher import PaperFetcher
        try:
            # The package parses some XML responses with the HTML parser on
            # purpose; bs4 warns about it on every call, which floods task logs.
            import warnings
            from bs4 import XMLParsedAsHTMLWarning
            warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        except Exception:
            pass
    except ImportError:
        if not _fetcher_import_warned:
            logger.warning(
                "paper_text_fetcher is not installed; full-text fetching is limited to "
                "the Europe PMC / NCBI PMC path. Rebuild the Airflow image to pick up "
                "airflow/requirements.txt."
            )
            _fetcher_import_warned = True
        return None

    class D3PaperFetcher(PaperFetcher):
        """
        paper-text-fetcher with D3's preprint-prefix fix.

        The package routes only ``10.1101/`` DOIs to its bioRxiv/medRxiv
        Chromium path. bioRxiv moved new deposits to the ``10.64898/`` prefix
        in 2026; those DOIs otherwise fall through to the journal sources and
        22 of 37 recent ones came back without a body. The page URL is built
        from the DOI, so the same path works for both prefixes.
        """

        @staticmethod
        def is_preprint_doi(doi: str) -> bool:
            return doi.startswith(PREPRINT_DOI_PREFIXES)

    wanted = Path(cache_dir) if cache_dir else fetcher_cache_dir()
    fetcher = getattr(_fetcher_local, "fetcher", None)
    if fetcher is None or getattr(_fetcher_local, "cache_dir", None) != wanted:
        wanted.mkdir(parents=True, exist_ok=True)
        contact = os.getenv("PAPER_FETCHER_CONTACT_EMAIL", "").strip() or None
        fetcher = D3PaperFetcher(
            cache_dir=wanted,
            contact_email=contact,
            tool_name=FETCHER_TOOL_NAME,
            api_keys=_fetcher_api_keys(),
            use_cache=True,
            verbose=False,
        )
        _fetcher_local.fetcher = fetcher
        _fetcher_local.cache_dir = wanted
    return fetcher


def _unavailable(reason: str) -> Dict[str, Any]:
    return {
        "text": None,
        "source": None,
        "status": TEXT_STATUS_UNAVAILABLE,
        "has_full_text": False,
        "reason": reason,
        "from_cache": False,
    }


def fetch_fulltext_detailed(
    doi: str,
    *,
    telemetry: Optional[Telemetry] = None,
    cache_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Fetch a paper through paper-text-fetcher and return its detailed result.

    Always returns the same six keys the package does:
    ``text, source, status, has_full_text, reason, from_cache``. When the
    package is not installed or the DOI is malformed, ``status`` is
    ``unavailable`` and ``reason`` says why, so callers never need a special
    case for "fetcher missing".
    """
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return _unavailable("invalid_doi")
    fetcher = get_paper_fetcher(cache_dir)
    if fetcher is None:
        return _unavailable("paper_text_fetcher_not_installed")

    if telemetry is not None:
        telemetry.total_requests += 1
    try:
        info = fetcher.get_paper_text_detailed(doi_norm) or {}
    except Exception as exc:
        logger.warning("paper-text-fetcher failed doi=%s err=%s", doi_norm, exc)
        if telemetry is not None:
            telemetry.api_retry_count += 1
        return _unavailable(f"fetch_error: {exc}")

    status = info.get("status") or (
        TEXT_STATUS_FULL if info.get("has_full_text") else TEXT_STATUS_UNAVAILABLE
    )
    text = strip_nul(info.get("text")) if status == TEXT_STATUS_FULL else None
    return {
        "text": text,
        "source": info.get("source") or None,
        "status": status,
        "has_full_text": status == TEXT_STATUS_FULL,
        "reason": info.get("reason"),
        "from_cache": bool(info.get("from_cache")),
    }


def fetcher_cache_key(doi: str, cache_dir: Optional[Path] = None) -> Optional[str]:
    """Path of the fetcher's cache file for ``doi``, relative to the cache dir."""
    doi_norm = normalize_doi(doi)
    fetcher = get_paper_fetcher(cache_dir) if doi_norm else None
    if fetcher is None:
        return None
    try:
        path = Path(fetcher.cache.path_for(doi_norm))
        return str(path.relative_to(Path(fetcher.cache.cache_dir)))
    except Exception:
        return None


def _read_cache_payload_text(path: Path) -> Optional[str]:
    """Read the ``full_text`` (or legacy ``text``) field of a mapping-DAG cache file."""
    if not path.is_file():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        logger.debug("Failed reading cached paper text at %s", path, exc_info=True)
        return None
    if not isinstance(payload, dict):
        return None
    for key in ("full_text", "text"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def load_cached_paper_text(
    cursor: Any,
    paper_doi: str,
    *,
    cache_dir: Optional[Path] = None,
) -> Optional[str]:
    """
    Return already-cached full text for a paper, or None. Never hits the network.

    Looks, in order, at
      1. the paper-mapping DAGs' JSON cache, via ``papers.fulltext_cache_key``
         probed under each DAG's output root (``mapping_output_roots``);
      2. paper-text-fetcher's own cache, but only entries it recorded as full
         text (a cached abstract is not a paper).
    """
    doi_norm = normalize_doi(paper_doi)
    if not doi_norm:
        return None

    cache_key: Optional[str] = None
    try:
        cursor.execute(
            "SELECT fulltext_cache_key FROM papers WHERE paper_doi = %s LIMIT 1;",
            (doi_norm,),
        )
        row = cursor.fetchone()
        if row:
            cache_key = row[0] if not isinstance(row, dict) else row.get("fulltext_cache_key")
    except Exception:
        logger.debug("papers lookup failed for %s", doi_norm, exc_info=True)

    if isinstance(cache_key, str) and cache_key.strip():
        for root in mapping_output_roots():
            text = _read_cache_payload_text(root / cache_key)
            if text:
                return strip_nul(text)

    fetcher = get_paper_fetcher(cache_dir)
    if fetcher is not None:
        try:
            cached = fetcher.cache.get(doi_norm)
        except Exception:
            cached = None
        if cached:
            text, _source, has_full_text = cached
            if has_full_text and isinstance(text, str) and text.strip():
                return strip_nul(text)
    return None
