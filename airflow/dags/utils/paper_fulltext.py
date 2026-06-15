"""
OA-only full text fetching (Option B).

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

from utils.find_reuse_core import Telemetry, normalize_doi

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
    """
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return None, "none", False, "invalid_doi"

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

