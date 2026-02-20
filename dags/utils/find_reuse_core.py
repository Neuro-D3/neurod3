"""
Core helper utilities adapted from ../find_reuse for resolving paper metadata.

This file is intentionally scoped:
- DOI normalization + extraction helpers
- throttled/retried HTTP JSON fetch helpers with telemetry
- Crossref/OpenAlex metadata lookups (title + openalex id)

It is designed to be called from Airflow tasks and to emit useful telemetry for
progress debugging, throttling, and API errors.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
import threading
import time
import warnings
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


# DOI pattern adapted from ../find_reuse/dandi_primary_papers.py.
# Matches `10.xxxx/...` and stops at whitespace or common punctuation that typically terminates a DOI.
DOI_REGEX = re.compile(r'10\.\d{4,}/[^\s\]\)>"\',;]+', flags=re.IGNORECASE)


def normalize_doi(doi: str) -> Optional[str]:
    if not doi or not isinstance(doi, str):
        return None
    d = doi.strip()
    if not d:
        return None
    # Strip common prefixes/urls
    for prefix in ("doi:", "DOI:"):
        if d.startswith(prefix):
            d = d[len(prefix) :].strip()
    if "doi.org/" in d:
        d = d.split("doi.org/")[-1].strip()
    # Trim trailing punctuation
    d = d.rstrip(" .;,)")
    d = d.lstrip("(")
    if not d.lower().startswith("10."):
        return None

    # Canonicalize common preprint DOI variants.
    # bioRxiv / medRxiv commonly appear with a trailing version suffix like `v1` which is NOT part of the DOI.
    # Example: `10.1101/2024.04.23.590673v1` -> `10.1101/2024.04.23.590673`
    if d.lower().startswith("10.1101/"):
        # Strip common suffixes that appear in free text but are not part of the DOI.
        # Examples:
        # - `...v1` -> `...`
        # - `...v4.abstract` -> `...`
        # - `....abstract` -> `...`
        d = re.sub(r"(?:v\d+)(?:\.(?:abstract|full|pdf))?$", "", d, flags=re.IGNORECASE)
        d = re.sub(r"\.(?:abstract|full|pdf)$", "", d, flags=re.IGNORECASE)
        # Guard against obviously incomplete year-only extractions.
        if re.fullmatch(r"10\.1101/\d{4}", d, flags=re.IGNORECASE):
            return None

    return d


def extract_dois_from_text(text: Optional[str]) -> list[str]:
    if not text:
        return []
    found = DOI_REGEX.findall(text)
    out: list[str] = []
    for raw in found:
        d = normalize_doi(raw)
        if d and d not in out:
            out.append(d)
    return out


_ZENODO_DOI_RE = re.compile(r"^10\.5281/zenodo\.(\d+)$", flags=re.IGNORECASE)


def _zenodo_record_id_from_doi(doi: str) -> Optional[int]:
    d = normalize_doi(doi) or ""
    m = _ZENODO_DOI_RE.match(d)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def resolve_zenodo_metadata(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> Dict[str, Any]:
    """
    Resolve metadata for Zenodo DOIs (10.5281/zenodo.<record_id>).

    Returns keys:
    - title: str | None
    - authors: list[dict] | None  (format: {"name": "...", ...})
    - url: str | None
    - record_id: int | None
    """
    record_id = _zenodo_record_id_from_doi(doi)
    if record_id is None:
        return {"title": None, "authors": None, "url": None, "record_id": None}

    url = f"https://zenodo.org/api/records/{record_id}"
    data = http_get_json(
        session,
        url,
        timeout=30,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    ) or {}

    md = data.get("metadata") if isinstance(data, dict) else None
    md = md if isinstance(md, dict) else {}

    title = md.get("title") if isinstance(md.get("title"), str) else None

    creators = md.get("creators")
    authors: Optional[List[Dict[str, Any]]] = None
    if isinstance(creators, list):
        out: List[Dict[str, Any]] = []
        for c in creators:
            if not isinstance(c, dict):
                continue
            name = c.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            entry: Dict[str, Any] = {"name": name.strip()}
            if isinstance(c.get("orcid"), str) and c.get("orcid").strip():
                entry["orcid"] = c.get("orcid").strip()
            if isinstance(c.get("affiliation"), str) and c.get("affiliation").strip():
                entry["affiliation"] = c.get("affiliation").strip()
            out.append(entry)
        authors = out or None

    links = data.get("links") if isinstance(data, dict) else None
    links = links if isinstance(links, dict) else {}
    html_url = links.get("html") if isinstance(links.get("html"), str) else None
    resolved_url = html_url or f"https://zenodo.org/records/{record_id}"

    return {"title": title, "authors": authors, "url": resolved_url, "record_id": record_id}


@dataclass
class Telemetry:
    api_429_count: int = 0
    api_5xx_count: int = 0
    api_retry_count: int = 0
    throttled_count: int = 0
    throttled_sleep_seconds: float = 0.0
    total_requests: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "api_429_count": self.api_429_count,
            "api_5xx_count": self.api_5xx_count,
            "api_retry_count": self.api_retry_count,
            "throttled_count": self.throttled_count,
            "throttled_sleep_seconds": self.throttled_sleep_seconds,
            "total_requests": self.total_requests,
        }


_rate_lock = threading.Lock()
_last_request_at = 0.0


def _throttle(min_interval_seconds: float, telemetry: Telemetry) -> None:
    global _last_request_at
    if min_interval_seconds <= 0:
        return
    with _rate_lock:
        now = time.monotonic()
        wait = min_interval_seconds - (now - _last_request_at)
        if wait > 0:
            telemetry.throttled_count += 1
            telemetry.throttled_sleep_seconds += float(wait)
            logger.debug("Throttling: sleep=%.3fs", wait)
            time.sleep(wait)
        _last_request_at = time.monotonic()


def http_get_json(
    session: requests.Session,
    url: str,
    *,
    timeout: int = 20,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
    telemetry: Optional[Telemetry] = None,
) -> Optional[Dict[str, Any]]:
    """
    Throttled + retried JSON GET with simple telemetry.

    Retries transient statuses: 429, 502, 503, 504 and network errors.
    """
    tel = telemetry or Telemetry()

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        _throttle(min_interval_seconds=min_interval_seconds, telemetry=tel)
        tel.total_requests += 1
        try:
            resp = session.get(url, timeout=timeout)
            status = resp.status_code

            if status in (429, 502, 503, 504):
                if status == 429:
                    tel.api_429_count += 1
                elif status >= 500:
                    tel.api_5xx_count += 1
                tel.api_retry_count += 1

                retry_after = resp.headers.get("Retry-After")
                wait = None
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = None
                if wait is None:
                    wait = min(backoff_seconds * (2 ** (attempt - 1)), 60.0)

                logger.warning(
                    "HTTP transient %s (attempt %d/%d). Sleeping %.1fs url=%s body=%s",
                    status,
                    attempt,
                    max_retries,
                    wait,
                    url,
                    (resp.text[:500] if resp.text else ""),
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError as e:
                logger.warning("Non-JSON response url=%s status=%s body=%s", url, status, resp.text[:500])
                last_exc = e
                break
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            tel.api_retry_count += 1
            wait = min(backoff_seconds * (2 ** (attempt - 1)), 60.0)
            logger.warning(
                "HTTP request failed (attempt %d/%d). Sleeping %.1fs url=%s error=%s",
                attempt,
                max_retries,
                wait,
                url,
                e,
            )
            time.sleep(wait)
            continue
        except requests.RequestException as e:
            last_exc = e
            logger.warning("HTTP request error url=%s error=%s", url, e)
            break

    if last_exc:
        logger.debug("http_get_json failed url=%s last_exc=%s", url, last_exc)
    return None


def resolve_crossref_title(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Optional[str]:
    warnings.warn(
        "resolve_crossref_title() is deprecated; use resolve_crossref_metadata() to avoid duplicate API calls.",
        DeprecationWarning,
        stacklevel=2,
    )
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return None
    url = f"https://api.crossref.org/works/{quote(doi_norm)}"
    data = http_get_json(
        session,
        url,
        timeout=20,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not data:
        return None
    msg = data.get("message") or {}
    title = msg.get("title")
    if isinstance(title, list) and title:
        t0 = title[0]
        return t0.strip() if isinstance(t0, str) and t0.strip() else None
    if isinstance(title, str) and title.strip():
        return title.strip()
    return None


def resolve_crossref_authors(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Optional[List[str]]:
    """
    Return list of author display names if available.
    Crossref shape: message.author = [{given, family, name, literal, ...}, ...]
    """
    warnings.warn(
        "resolve_crossref_authors() is deprecated; use resolve_crossref_metadata() to avoid duplicate API calls.",
        DeprecationWarning,
        stacklevel=2,
    )
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return None
    url = f"https://api.crossref.org/works/{quote(doi_norm)}"
    data = http_get_json(
        session,
        url,
        timeout=20,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not data:
        return None
    msg = data.get("message") or {}
    authors = msg.get("author")
    if not isinstance(authors, list) or not authors:
        return None

    out: List[str] = []
    for a in authors:
        if not isinstance(a, dict):
            continue
        # Prefer explicit 'name' or 'literal' if provided
        for key in ("name", "literal"):
            v = a.get(key)
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
                break
        else:
            given = a.get("given")
            family = a.get("family")
            parts = []
            if isinstance(given, str) and given.strip():
                parts.append(given.strip())
            if isinstance(family, str) and family.strip():
                parts.append(family.strip())
            if parts:
                out.append(" ".join(parts))

    # Deduplicate while preserving order
    deduped: List[str] = []
    for n in out:
        if n not in deduped:
            deduped.append(n)
    return deduped or None


def resolve_openalex_metadata(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Dict[str, Optional[str]]:
    """
    Return {openalex_id, title} when possible.
    """
    warnings.warn(
        "resolve_openalex_metadata() is deprecated; use resolve_openalex_work() to avoid duplicate API calls.",
        DeprecationWarning,
        stacklevel=2,
    )
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return {"openalex_id": None, "title": None}
    url = f"https://api.openalex.org/works/doi:{doi_norm}"
    data = http_get_json(
        session,
        url,
        timeout=20,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not data:
        return {"openalex_id": None, "title": None}
    title = data.get("title")
    if isinstance(title, str):
        title = title.strip() or None
    else:
        title = None
    openalex_id = data.get("id")
    if not isinstance(openalex_id, str) or not openalex_id.strip():
        openalex_id = None
    return {"openalex_id": openalex_id, "title": title}


def resolve_openalex_authors(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Optional[List[str]]:
    """
    Return list of author display names if available.
    OpenAlex shape: authorships = [{author: {display_name}}, ...]
    """
    warnings.warn(
        "resolve_openalex_authors() is deprecated; use resolve_openalex_work() to avoid duplicate API calls.",
        DeprecationWarning,
        stacklevel=2,
    )
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return None
    url = f"https://api.openalex.org/works/doi:{doi_norm}"
    data = http_get_json(
        session,
        url,
        timeout=20,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not data:
        return None
    authorships = data.get("authorships")
    if not isinstance(authorships, list) or not authorships:
        return None

    out: List[str] = []
    for a in authorships:
        if not isinstance(a, dict):
            continue
        author = a.get("author")
        if isinstance(author, dict):
            dn = author.get("display_name")
            if isinstance(dn, str) and dn.strip():
                out.append(dn.strip())
    # Deduplicate while preserving order
    deduped: List[str] = []
    for n in out:
        if n not in deduped:
            deduped.append(n)
    return deduped or None


def resolve_crossref_metadata(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Dict[str, Any]:
    """
    Fetch Crossref once and return both title + authors.

    Returns:
      { "title": Optional[str], "authors": Optional[List[str]] }
    """
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return {"title": None, "authors": None}
    url = f"https://api.crossref.org/works/{quote(doi_norm)}"
    data = http_get_json(
        session,
        url,
        timeout=20,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not data:
        return {"title": None, "authors": None}
    msg = data.get("message") or {}

    # Title
    title_out: Optional[str] = None
    title = msg.get("title")
    if isinstance(title, list) and title:
        t0 = title[0]
        title_out = t0.strip() if isinstance(t0, str) and t0.strip() else None
    elif isinstance(title, str) and title.strip():
        title_out = title.strip()

    # Authors
    authors_out: Optional[List[str]] = None
    authors = msg.get("author")
    if isinstance(authors, list) and authors:
        out: List[str] = []
        for a in authors:
            if not isinstance(a, dict):
                continue
            for key in ("name", "literal"):
                v = a.get(key)
                if isinstance(v, str) and v.strip():
                    out.append(v.strip())
                    break
            else:
                given = a.get("given")
                family = a.get("family")
                parts = []
                if isinstance(given, str) and given.strip():
                    parts.append(given.strip())
                if isinstance(family, str) and family.strip():
                    parts.append(family.strip())
                if parts:
                    out.append(" ".join(parts))

        deduped: List[str] = []
        for n in out:
            if n not in deduped:
                deduped.append(n)
        authors_out = deduped or None

    return {"title": title_out, "authors": authors_out}


def resolve_openalex_work(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Dict[str, Any]:
    """
    Fetch OpenAlex once and return id + title + authors.

    Returns:
      { "openalex_id": Optional[str], "title": Optional[str], "authors": Optional[List[str]] }
    """
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return {"openalex_id": None, "title": None, "authors": None}
    url = f"https://api.openalex.org/works/doi:{doi_norm}"
    data = http_get_json(
        session,
        url,
        timeout=20,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not data:
        return {"openalex_id": None, "title": None, "authors": None}

    title = data.get("title")
    title_out = title.strip() if isinstance(title, str) and title.strip() else None

    openalex_id = data.get("id")
    openalex_id_out = openalex_id.strip() if isinstance(openalex_id, str) and openalex_id.strip() else None

    authorships = data.get("authorships")
    authors_out: Optional[List[str]] = None
    if isinstance(authorships, list) and authorships:
        out: List[str] = []
        for a in authorships:
            if not isinstance(a, dict):
                continue
            author = a.get("author")
            if isinstance(author, dict):
                dn = author.get("display_name")
                if isinstance(dn, str) and dn.strip():
                    out.append(dn.strip())
        deduped: List[str] = []
        for n in out:
            if n not in deduped:
                deduped.append(n)
        authors_out = deduped or None

    return {"openalex_id": openalex_id_out, "title": title_out, "authors": authors_out}

