"""
Backfill papers.author_ids and papers.author_orcids from OpenAlex.

The paper-mapping DAGs fill author ids for their new papers at the end of each
run (their fill_author_ids task). This DAG does the same for every stored
paper that lacks them: run it once after deploying the columns, or whenever
papers were stored without ids (a run that stopped early, a disabled
fill_author_ids). Manual only; see utils/paper_author_ids.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
from typing import Any, Dict

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:  # pragma: no cover
    from airflow.operators.python import PythonOperator  # type: ignore

from utils.openalex_budget import check_openalex_budget
from utils.paper_author_ids import fill_missing_author_ids

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


def backfill_author_ids(**context) -> Dict[str, Any]:
    params = context.get("params") or {}
    check_openalex_budget(params)
    return fill_missing_author_ids(
        max_papers=int(params.get("max_papers") or 20000),
        recheck_after_days=int(params.get("recheck_after_days") or 30),
        min_interval_seconds=float(params.get("min_api_interval_seconds") or 0.2),
    )


dag = DAG(
    "paper_author_ids_backfill",
    default_args=default_args,
    description="Backfill papers.author_ids / author_orcids from OpenAlex for every stored paper",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["papers", "openalex", "metrics", "backfill"],
    params={
        # Papers looked up per run (50 per OpenAlex request, so 20000 is 400 requests).
        "max_papers": 20000,
        # A paper OpenAlex did not know is looked up again after this many days.
        "recheck_after_days": 30,
        # Abort before looking anything up when fewer OpenAlex requests remain today.
        "min_openalex_requests": 200,
        "min_api_interval_seconds": 0.2,
    },
)

backfill_task = PythonOperator(task_id="backfill_author_ids", python_callable=backfill_author_ids, dag=dag)
