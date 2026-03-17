"""
Placeholder DAG for future paper reuse classification.

This DAG intentionally does not perform any LLM/API-token work yet. It exists so
the downstream classification stage has a stable Airflow home once credentials,
retry policy, and cost controls are defined.
"""

from __future__ import annotations

from datetime import datetime
import logging

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:  # pragma: no cover
    from airflow.operators.python import PythonOperator  # type: ignore

logger = logging.getLogger(__name__)


def classification_placeholder(**context) -> None:
    logger.info(
        "Paper reuse classification placeholder DAG invoked for run_id=%s. "
        "No classification work is performed until API tokens and model settings are configured.",
        context.get("run_id"),
    )


dag = DAG(
    "paper_reuse_classification_placeholder",
    default_args={
        "owner": "neurod3",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="Paused placeholder for future paper reuse classification",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["datasets", "papers", "classification", "placeholder"],
)

PythonOperator(
    task_id="classification_placeholder",
    python_callable=classification_placeholder,
    dag=dag,
)
