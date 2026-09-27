"""
OpenAlex author ids (and ORCIDs) for stored papers.

The dataset metrics count a reuse as the dataset's own lab when the citing
paper shares an author with the dataset's primary papers. Names are a loose
match (common names collide, initials and name changes miss), so papers also
keep OpenAlex's stable author ids: papers.author_ids, papers.author_orcids.
The paper_author_ids DAG fills them, 50 papers per OpenAlex request.
"""

from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import requests

from utils.find_reuse_core import Telemetry, http_get_json

# OpenAlex returns at most this many works per page, and a doi filter takes at
# most this many values.
BATCH_SIZE = 50

# Columns on papers; author_ids_checked_at records the lookup, so a paper
# OpenAlex does not know is not asked about every day.
PAPER_AUTHOR_ID_DDL = """
ALTER TABLE papers ADD COLUMN IF NOT EXISTS author_ids JSONB;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS author_orcids JSONB;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS author_ids_checked_at TIMESTAMPTZ;
"""


def _tail(url: Any, marker: str) -> Optional[str]:
    """`https://openalex.org/A5023888391` -> `A5023888391` (same for orcid.org URLs)."""
    if not isinstance(url, str) or not url.strip():
        return None
    value = url.strip()
    return value.split(marker, 1)[1] if marker in value else value


def author_ids_from_work(work: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """The work's OpenAlex author ids and ORCIDs, in author order, without duplicates."""
    ids: List[str] = []
    orcids: List[str] = []
    for authorship in work.get("authorships") or []:
        author = authorship.get("author") if isinstance(authorship, dict) else None
        if not isinstance(author, dict):
            continue
        author_id = _tail(author.get("id"), "openalex.org/")
        orcid = _tail(author.get("orcid"), "orcid.org/")
        if author_id and author_id not in ids:
            ids.append(author_id)
        if orcid and orcid not in orcids:
            orcids.append(orcid)
    return ids, orcids


def _doi_of(work: Dict[str, Any]) -> Optional[str]:
    doi = work.get("doi")
    if not isinstance(doi, str) or not doi.strip():
        return None
    return _tail(doi.strip().lower(), "doi.org/")


def filterable(doi: str) -> bool:
    """A DOI that can go in an OpenAlex doi filter (`|` separates values, `,` filters)."""
    return bool(doi) and not any(ch in doi for ch in "|,")


def works_url(dois: Iterable[str]) -> str:
    values = "|".join(quote(d, safe="/:.-_()") for d in dois)
    return (
        f"https://api.openalex.org/works?filter=doi:{values}"
        f"&per-page={BATCH_SIZE}&select=doi,authorships"
    )


def fetch_author_ids(
    session: requests.Session,
    dois: List[str],
    *,
    telemetry: Telemetry,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> Dict[str, Tuple[List[str], List[str]]]:
    """
    Lower-cased DOI -> (author ids, ORCIDs) for the DOIs OpenAlex knows, one
    request per BATCH_SIZE DOIs. DOIs it does not know are simply absent.
    Raises ApiQuotaExhausted (from http_get_json) when the daily budget is spent.
    """
    found: Dict[str, Tuple[List[str], List[str]]] = {}
    usable = [d for d in dois if filterable(d)]
    for start in range(0, len(usable), BATCH_SIZE):
        batch = usable[start:start + BATCH_SIZE]
        data = http_get_json(
            session,
            works_url(batch),
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
            telemetry=telemetry,
        )
        for work in (data or {}).get("results") or []:
            doi = _doi_of(work)
            if doi:
                found[doi] = author_ids_from_work(work)
    return found
