"""
Whether a primary paper's citing papers were fetched completely.

Citing papers are fetched only for the datasets a mapping run selects, and the
nightly run selects unmapped datasets. A dataset is mapped as soon as its
primary paper is resolved, so if OpenAlex stopped answering halfway through the
citing-paper list, the list used to be saved as if complete and never fetched
again.

Each ``<x>_paper_map`` row (one per dataset and primary paper) now records:

- ``citations_status``:
  - ``complete``: every citing-paper page and version was fetched.
  - ``truncated``: the fetch worked but stopped at
    ``max_citing_papers_per_primary`` with more citing papers left, so the
    list is known to be short. It is not retried; a run with a higher cap
    and ``include_already_mapped`` fetches the rest.
  - ``failed``: a citation source did not answer (``CitationFetchError``).
    The list of the version being fetched is not saved; a version fetched
    before it keeps its list.
  - NULL: never fetched, or there was nothing to ask OpenAlex about (the
    paper has no OpenAlex id and no other version, citation enrichment turned
    off, rows from before this column).
- ``citations_checked_at``: when that status was set.
- ``citations_error``: the error for a ``failed`` row, cleared on success.
- ``citations_attempts``: how many runs in a row the source answered with an
  error of its own (a 400, a 403, a 501, a body that is not JSON); reset to 0
  on success. A run where the source was only busy or unreachable (429, 500,
  502, 503, 504, a timeout, a dropped connection) marks the row ``failed`` but
  is not counted, so an outage of several nights does not use the attempts up.

The nightly selection picks datasets with a ``failed`` row as well as unmapped
ones, so a failed fetch is retried on the next run. A row with
``MAX_CITATION_ATTEMPTS`` counted failures in a row is left alone: it keeps its
status and error for a look, but no longer sorts ahead of unmapped datasets
every night. ``include_already_mapped`` runs still fetch it.
"""

from __future__ import annotations

from typing import Optional

STATUS_COMPLETE = "complete"
STATUS_TRUNCATED = "truncated"
STATUS_FAILED = "failed"

# A row with this many counted failures in a row is no longer selected for retry.
MAX_CITATION_ATTEMPTS = 3

# Error text is kept short enough to read in a table, long enough to show the URL.
_MAX_ERROR_CHARS = 1000


def citation_status_ddl(map_table: str) -> str:
    """Columns added to ``<x>_paper_map``; run through ``apply_schema_ddl``."""
    return f"""
    ALTER TABLE {map_table} ADD COLUMN IF NOT EXISTS citations_status TEXT;
    ALTER TABLE {map_table} ADD COLUMN IF NOT EXISTS citations_checked_at TIMESTAMPTZ;
    ALTER TABLE {map_table} ADD COLUMN IF NOT EXISTS citations_error TEXT;
    ALTER TABLE {map_table} ADD COLUMN IF NOT EXISTS citations_attempts INTEGER;
    """


def fetched_citations_status(versions_fetched: int, truncated: bool) -> Optional[str]:
    """
    The status of a fetch that raised nothing: None when no version of the
    paper had an OpenAlex id to fetch citing papers for, ``truncated`` when a
    list stopped at the cap, otherwise ``complete``.
    """
    if versions_fetched <= 0:
        return None
    return STATUS_TRUNCATED if truncated else STATUS_COMPLETE


def mark_citations(cursor, map_table: str, id_col: str, dataset_id: str, paper_doi: str,
                   status: Optional[str], error: Optional[str] = None, *, count_attempt: bool = True) -> None:
    """
    Set one (dataset, primary paper) row's citation status. The caller commits.
    A status of None clears it (nothing could be fetched), so a row that failed
    earlier is not retried for a list that does not exist.

    A failure adds one to ``citations_attempts`` unless ``count_attempt`` is
    False (pass ``not error.transient``: a busy or unreachable source is not
    counted); a success resets it to 0.
    """
    if status not in (None, STATUS_COMPLETE, STATUS_TRUNCATED, STATUS_FAILED):
        raise ValueError(f"unknown citations_status {status!r}")
    error_text = None
    if status == STATUS_FAILED and error:
        error_text = error[:_MAX_ERROR_CHARS]
    if status != STATUS_FAILED:
        attempts_sql = "0"
    elif count_attempt:
        attempts_sql = "COALESCE(citations_attempts, 0) + 1"
    else:
        attempts_sql = "COALESCE(citations_attempts, 0)"
    cursor.execute(
        f"""
        UPDATE {map_table}
        SET citations_status = %s, citations_checked_at = NOW(), citations_error = %s,
            citations_attempts = {attempts_sql}
        WHERE {id_col} = %s AND paper_doi = %s;
        """,
        (status, error_text, dataset_id, paper_doi),
    )


def already_fetched(status: Optional[str]) -> bool:
    """
    Whether a row's citing papers are in and need no fetch on a retry run:
    ``complete``, or ``truncated`` (short only because of the cap). A dataset
    is selected again when one of its primary papers failed; its other
    primaries are skipped with this, so a retry asks OpenAlex only for the
    lists that are missing.
    """
    return status in (STATUS_COMPLETE, STATUS_TRUNCATED)


def has_failed_citations_sql(map_table: str, id_col: str, dataset_alias: str = "d") -> str:
    """
    SQL condition: the dataset has a primary paper whose citing papers failed to
    fetch and has not yet failed ``MAX_CITATION_ATTEMPTS`` times.
    """
    return (
        f"EXISTS (SELECT 1 FROM {map_table} cf "
        f"WHERE cf.{id_col} = {dataset_alias}.dataset_id AND cf.citations_status = '{STATUS_FAILED}' "
        f"AND COALESCE(cf.citations_attempts, 0) < {MAX_CITATION_ATTEMPTS})"
    )


def count_past_retry_limit_sql(map_table: str) -> str:
    """SQL: how many ``failed`` rows the nightly selection no longer retries."""
    return (
        f"SELECT COUNT(*) FROM {map_table} "
        f"WHERE citations_status = '{STATUS_FAILED}' AND COALESCE(citations_attempts, 0) >= {MAX_CITATION_ATTEMPTS}"
    )
