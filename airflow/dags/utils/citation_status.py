"""
Whether a primary paper's citing papers were fetched completely.

Citing papers are fetched only for the datasets a mapping run selects, and the
nightly run selects unmapped datasets. A dataset is mapped as soon as its
primary paper is resolved, so if OpenAlex stopped answering halfway through the
citing-paper list, the list used to be saved as if complete and never fetched
again.

Each ``<x>_paper_map`` row (one per dataset and primary paper) now records:

- ``citations_status``: ``complete`` when every citing-paper page and version
  was fetched; ``failed`` when OpenAlex did not answer (``OpenAlexError``), in
  which case none of that list was saved; NULL when never fetched or nothing
  could be fetched (no OpenAlex id, citation enrichment turned off, rows from
  before this column).
- ``citations_checked_at``: when that status was set.
- ``citations_error``: the error for a ``failed`` row, cleared on success.

The nightly selection picks datasets with a ``failed`` row as well as unmapped
ones, so a failed fetch is retried on the next run (and every run after, until
it completes).
"""

from __future__ import annotations

from typing import Optional

STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"

# Error text is kept short enough to read in a table, long enough to show the URL.
_MAX_ERROR_CHARS = 1000


def citation_status_ddl(map_table: str) -> str:
    """Columns added to ``<x>_paper_map``; run through ``apply_schema_ddl``."""
    return f"""
    ALTER TABLE {map_table} ADD COLUMN IF NOT EXISTS citations_status TEXT;
    ALTER TABLE {map_table} ADD COLUMN IF NOT EXISTS citations_checked_at TIMESTAMPTZ;
    ALTER TABLE {map_table} ADD COLUMN IF NOT EXISTS citations_error TEXT;
    """


def mark_citations(cursor, map_table: str, id_col: str, dataset_id: str, paper_doi: str,
                   status: str, error: Optional[str] = None) -> None:
    """Set one (dataset, primary paper) row's citation status. The caller commits."""
    if status not in (STATUS_COMPLETE, STATUS_FAILED):
        raise ValueError(f"unknown citations_status {status!r}")
    error_text = None
    if status == STATUS_FAILED and error:
        error_text = error[:_MAX_ERROR_CHARS]
    cursor.execute(
        f"""
        UPDATE {map_table}
        SET citations_status = %s, citations_checked_at = NOW(), citations_error = %s
        WHERE {id_col} = %s AND paper_doi = %s;
        """,
        (status, error_text, dataset_id, paper_doi),
    )


def has_failed_citations_sql(map_table: str, id_col: str, dataset_alias: str = "d") -> str:
    """SQL condition: the dataset has a primary paper whose citing papers failed to fetch."""
    return (
        f"EXISTS (SELECT 1 FROM {map_table} cf "
        f"WHERE cf.{id_col} = {dataset_alias}.dataset_id AND cf.citations_status = '{STATUS_FAILED}')"
    )
