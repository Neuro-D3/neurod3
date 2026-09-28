"""
OpenAlex author ids (and ORCIDs) for stored papers.

The dataset metrics count a reuse as the dataset's own lab when the citing
paper shares an author with the dataset's primary papers. Names are a loose
match (common names collide, initials and name changes miss), so papers also
keep OpenAlex's stable author ids: papers.author_ids, papers.author_orcids.

Each paper-mapping DAG fills them for its archive's new papers at the end of a
run (its fill_author_ids task); the paper_author_ids_backfill DAG fills every
stored paper that lacks them. Both call fill_missing_author_ids: 50 papers per
OpenAlex request.
"""

import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote

import requests

from utils.batch_progress import BatchProgress
from utils.find_reuse_core import ApiQuotaExhausted, Telemetry, http_get_json
from utils.openalex_budget import fetch_openalex_budget, format_budget

logger = logging.getLogger(__name__)

# OpenAlex returns at most this many works per page, and a doi filter takes at
# most this many values.
BATCH_SIZE = 50

ARCHIVES: Tuple[str, ...] = ("dandi", "openneuro", "crcns", "sparc")

# Columns on papers; author_ids_checked_at records the lookup, so a paper
# OpenAlex does not know is not asked about on every run.
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
) -> Tuple[Dict[str, Tuple[List[str], List[str]]], Set[str]]:
    """
    (found, answered), one request per BATCH_SIZE DOIs. `found` is lower-cased
    DOI -> (author ids, ORCIDs) for the DOIs OpenAlex knows; `answered` holds
    every DOI whose request got an answer, found or not, so a request that
    failed is not taken for "OpenAlex does not know these". DOIs that cannot go
    in a filter are answered as unknown. Raises ApiQuotaExhausted (from
    http_get_json) when the daily budget is spent.
    """
    found: Dict[str, Tuple[List[str], List[str]]] = {}
    answered: Set[str] = {d for d in dois if not filterable(d)}
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
        if data is None:
            logger.warning("OpenAlex did not answer for %d DOIs; they stay unchecked", len(batch))
            continue
        answered.update(batch)
        for work in data.get("results") or []:
            doi = _doi_of(work)
            if doi:
                found[doi] = author_ids_from_work(work)
    return found, answered


def select_papers_sql(
    scope_tables: Sequence[Tuple[str, str]], map_tables: Sequence[str], label_tables: Sequence[str]
) -> str:
    """
    Papers to look up: never looked up, or looked up without a result more than
    recheck_after_days ago (first %s); LIMIT is the second %s. `scope_tables`
    are (table, DOI column) pairs a paper must appear in, one archive's map and
    citation tables; none means every paper. Never-looked-up come first, then
    primary papers (`map_tables`), then papers labelled REUSE (`label_tables`).
    """
    scope = " OR ".join(f"EXISTS (SELECT 1 FROM {t} s WHERE s.{col} = p.paper_doi)" for t, col in scope_tables)
    primary = " OR ".join(f"EXISTS (SELECT 1 FROM {t} m WHERE m.paper_doi = p.paper_doi)" for t in map_tables)
    reused = " OR ".join(
        f"EXISTS (SELECT 1 FROM {t} c WHERE c.citing_paper_doi = p.paper_doi AND c.classification = 'REUSE')"
        for t in label_tables
    )
    return f"""
        SELECT p.paper_doi
        FROM papers p
        WHERE (p.author_ids_checked_at IS NULL
               OR (p.author_ids IS NULL AND p.author_ids_checked_at < NOW() - make_interval(days => %s)))
          {f"AND ({scope})" if scope else ""}
        ORDER BY
            (p.author_ids_checked_at IS NULL) DESC,
            ({primary or 'FALSE'}) DESC,
            ({reused or 'FALSE'}) DESC,
            p.paper_doi
        LIMIT %s;
    """


def _existing_tables(cursor, names: Sequence[str]) -> List[str]:
    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ANY(%s);",
        (list(names),),
    )
    present = {r[0] for r in cursor.fetchall()}
    return [n for n in names if n in present]


def fill_missing_author_ids(
    *,
    archives: Optional[Sequence[str]] = None,
    max_papers: int = 20000,
    recheck_after_days: int = 30,
    min_interval_seconds: float = 0.2,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    Look up and store author ids for papers that lack them: the papers of
    `archives` (their paper map and citation tables), or every paper when
    None. Stops early, without failing, when the OpenAlex budget runs out.
    """
    from utils.database import apply_schema_ddl, get_db_connection

    with get_db_connection() as conn:
        cursor = conn.cursor()
        apply_schema_ddl(cursor, PAPER_AUTHOR_ID_DDL)
        conn.commit()
        chosen = list(archives) if archives else list(ARCHIVES)
        map_tables = _existing_tables(cursor, [f"{a}_paper_map" for a in chosen])
        label_tables = _existing_tables(cursor, [f"{a}_paper_citation_classifications" for a in chosen])
        scope_tables: List[Tuple[str, str]] = []
        if archives:
            citation_tables = _existing_tables(cursor, [f"{a}_paper_citations" for a in archives])
            scope_tables = [(t, "paper_doi") for t in map_tables] + [(t, "citing_paper_doi") for t in citation_tables]
        if archives and not scope_tables:
            dois: List[str] = []
        else:
            cursor.execute(select_papers_sql(scope_tables, map_tables, label_tables), (recheck_after_days, max_papers))
            dois = [r[0] for r in cursor.fetchall()]

    telemetry = Telemetry()
    session = session or requests.Session()
    stats: Dict[str, Any] = {"papers": len(dois), "with_author_ids": 0, "not_in_openalex": 0,
                             "unanswered": 0, "stopped_early": False}
    progress = BatchProgress("author ids", total=len(dois), unit="papers", counters=stats, telemetry=telemetry)
    progress.update(0, note="starting", force=True)
    for start in range(0, len(dois), BATCH_SIZE):
        batch = dois[start:start + BATCH_SIZE]
        try:
            found, answered = fetch_author_ids(session, batch, telemetry=telemetry,
                                               min_interval_seconds=min_interval_seconds)
        except ApiQuotaExhausted:
            logger.warning("OpenAlex budget spent: stopping after %d of %d papers", start, len(dois))
            stats["stopped_early"] = True
            break
        with get_db_connection() as conn:
            cursor = conn.cursor()
            for doi in batch:
                if doi not in answered:
                    stats["unanswered"] += 1
                    continue
                ids, orcids = found.get(doi.lower(), (None, None))
                cursor.execute(
                    """
                    UPDATE papers
                    SET author_ids = %s::jsonb, author_orcids = %s::jsonb, author_ids_checked_at = NOW()
                    WHERE paper_doi = %s;
                    """,
                    (json.dumps(ids) if ids is not None else None,
                     json.dumps(orcids) if orcids is not None else None, doi),
                )
                stats["with_author_ids" if ids is not None else "not_in_openalex"] += 1
        progress.update(done=start + len(batch))
    note = "done"
    if dois:
        # Where today's OpenAlex budget stands now: one more metered request, so
        # skipped when there was nothing to look up.
        budget = fetch_openalex_budget(session, telemetry=telemetry)
        stats["openalex_remaining"] = budget.get("remaining")
        note = f"done. {format_budget(budget)}"
    progress.update(force=True, note=note)
    stats["openalex_requests"] = telemetry.total_requests
    logger.info("Author ids: %s", stats)
    return stats
