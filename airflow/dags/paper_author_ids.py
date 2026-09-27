"""
Fill papers.author_ids and papers.author_orcids from OpenAlex.

The dataset metrics use them to tell a dataset's own lab from independent
reuse (see utils/paper_author_ids.py). Each run looks up papers never looked up
before, the datasets' primary papers and reused papers first, up to
max_papers, 50 per OpenAlex request; a paper OpenAlex did not know is tried
again after recheck_after_days. The first run backfills every stored paper;
after that it picks up what mapping added.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
from typing import Any, Dict, List

import requests

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:  # pragma: no cover
    from airflow.operators.python import PythonOperator  # type: ignore

from utils.batch_progress import BatchProgress
from utils.database import apply_schema_ddl, get_db_connection
from utils.find_reuse_core import ApiQuotaExhausted, Telemetry
from utils.openalex_budget import check_openalex_budget
from utils.paper_author_ids import BATCH_SIZE, PAPER_AUTHOR_ID_DDL, fetch_author_ids

logger = logging.getLogger(__name__)

default_args = {
    "owner": "neurod3",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ARCHIVES = ("dandi", "openneuro", "crcns", "sparc")


def _existing_tables(cursor, names: List[str]) -> List[str]:
    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ANY(%s);",
        (names,),
    )
    present = {r[0] for r in cursor.fetchall()}
    return [n for n in names if n in present]


def select_papers_sql(map_tables: List[str], label_tables: List[str]) -> str:
    """
    Papers to look up: never looked up, or looked up without a result more than
    recheck_after_days ago (%s, then LIMIT %s). Never-looked-up first, then the
    datasets' primary papers, then papers labelled REUSE.
    """
    primary = " OR ".join(f"EXISTS (SELECT 1 FROM {t} m WHERE m.paper_doi = p.paper_doi)" for t in map_tables)
    reused = " OR ".join(
        f"EXISTS (SELECT 1 FROM {t} c WHERE c.citing_paper_doi = p.paper_doi AND c.classification = 'REUSE')"
        for t in label_tables
    )
    return f"""
        SELECT p.paper_doi
        FROM papers p
        WHERE p.author_ids_checked_at IS NULL
           OR (p.author_ids IS NULL AND p.author_ids_checked_at < NOW() - make_interval(days => %s))
        ORDER BY
            (p.author_ids_checked_at IS NULL) DESC,
            ({primary or 'FALSE'}) DESC,
            ({reused or 'FALSE'}) DESC,
            p.paper_doi
        LIMIT %s;
    """


def ensure_author_id_columns(**context) -> None:
    with get_db_connection() as conn:
        apply_schema_ddl(conn.cursor(), PAPER_AUTHOR_ID_DDL)


def fill_author_ids(**context) -> Dict[str, Any]:
    params = context.get("params") or {}
    check_openalex_budget(params)
    max_papers = int(params.get("max_papers") or 5000)
    recheck_after_days = int(params.get("recheck_after_days") or 30)

    with get_db_connection() as conn:
        cursor = conn.cursor()
        map_tables = _existing_tables(cursor, [f"{a}_paper_map" for a in ARCHIVES])
        label_tables = _existing_tables(cursor, [f"{a}_paper_citation_classifications" for a in ARCHIVES])
        cursor.execute(select_papers_sql(map_tables, label_tables), (recheck_after_days, max_papers))
        dois = [r[0] for r in cursor.fetchall()]

    telemetry = Telemetry()
    session = requests.Session()
    stats = {"papers": len(dois), "with_author_ids": 0, "not_in_openalex": 0, "stopped_early": False}
    progress = BatchProgress("author ids", total=len(dois), unit="papers", counters=stats, telemetry=telemetry)
    progress.line("starting")
    for start in range(0, len(dois), BATCH_SIZE):
        batch = dois[start:start + BATCH_SIZE]
        try:
            found = fetch_author_ids(session, batch, telemetry=telemetry,
                                     min_interval_seconds=float(params.get("min_api_interval_seconds") or 0.2))
        except ApiQuotaExhausted:
            logger.warning("OpenAlex budget spent: stopping after %d of %d papers", start, len(dois))
            stats["stopped_early"] = True
            break
        with get_db_connection() as conn:
            cursor = conn.cursor()
            for doi in batch:
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
    progress.update(force=True, note="done")
    stats["openalex_requests"] = telemetry.total_requests
    logger.info("Author ids: %s", stats)
    return stats


dag = DAG(
    "paper_author_ids",
    default_args=default_args,
    description="Fill papers.author_ids / author_orcids from OpenAlex (same-lab reuse)",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["papers", "openalex", "metrics"],
    is_paused_upon_creation=False,
    params={
        # Papers looked up per run (50 per OpenAlex request).
        "max_papers": 5000,
        # A paper OpenAlex did not know is looked up again after this many days.
        "recheck_after_days": 30,
        # Abort before looking anything up when fewer OpenAlex requests remain today.
        "min_openalex_requests": 200,
        "min_api_interval_seconds": 0.2,
    },
)

ensure_columns_task = PythonOperator(
    task_id="ensure_author_id_columns", python_callable=ensure_author_id_columns, dag=dag,
)
fill_task = PythonOperator(task_id="fill_author_ids", python_callable=fill_author_ids, dag=dag)

ensure_columns_task >> fill_task
