"""
OpenNeuro classification benchmark DAG.

Compares the ground-truth CSV (OpenNeuro_Datasets_BM.csv) against the
openneuro_paper_citations table to measure how well the citation pipeline
is capturing known dataset mentions.

Reports:
  - How many benchmark (DOI, dataset) pairs exist in the DB
  - Hit / miss breakdown
  - Classification distribution for matched edges
  - Detailed lists of hits and misses

Trigger manually from the Airflow UI.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:
    from airflow.operators.python import PythonOperator

try:
    from utils.database import get_db_connection
except ImportError:
    from dags.utils.database import get_db_connection

logger = logging.getLogger(__name__)

BM_CSV_PATH = os.path.join(os.path.dirname(__file__), "OpenNeuro_Datasets_BM.csv")


def run_benchmark(**context):
    """Load the benchmark CSV and check each (DOI, dataset) pair against the DB."""

    # --- 1. Load ground-truth CSV ---
    ground_truth: List[Dict[str, str]] = []
    with open(BM_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doi = (row.get("DOI") or "").strip()
            dataset_id = (row.get("Dataset Mention") or "").strip()
            title = (row.get("Title") or "").strip()
            if not dataset_id:
                continue
            ground_truth.append({
                "title": title,
                "doi": doi,
                "dataset_id": dataset_id,
            })

    logger.info("Loaded %d ground-truth rows from %s", len(ground_truth), BM_CSV_PATH)

    # --- 2. Check each pair against the DB ---
    hits: List[Dict[str, Any]] = []
    misses: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    with get_db_connection() as conn:
        cursor = conn.cursor()

        for entry in ground_truth:
            doi = entry["doi"]
            dataset_id = entry["dataset_id"]

            # Skip rows with no DOI (marked n/a in the CSV)
            if not doi or doi.lower() == "n/a":
                skipped.append(entry)
                continue

            # Check if this DOI appears as a citing_paper_doi for this dataset
            cursor.execute("""
                SELECT
                    cit.citing_paper_doi,
                    cls.classification,
                    cls.confidence,
                    cls.status
                FROM openneuro_paper_citations cit
                LEFT JOIN openneuro_paper_citation_classifications cls
                    ON  cls.openneuro_id        = cit.openneuro_id
                    AND cls.primary_paper_doi    = cit.primary_paper_doi
                    AND cls.citing_paper_doi     = cit.citing_paper_doi
                WHERE cit.openneuro_id = %s
                  AND cit.citing_paper_doi = %s
                LIMIT 1;
            """, (dataset_id, doi))

            row = cursor.fetchone()
            if row:
                cols = [d[0] for d in cursor.description]
                match = dict(zip(cols, row))
                hits.append({**entry, **match})
            else:
                # Also check if the DOI appears as a primary_paper_doi
                cursor.execute("""
                    SELECT paper_doi
                    FROM openneuro_paper_map
                    WHERE openneuro_id = %s AND paper_doi = %s
                    LIMIT 1;
                """, (dataset_id, doi))
                primary_row = cursor.fetchone()
                misses.append({
                    **entry,
                    "in_paper_map": primary_row is not None,
                })

    # --- 3. Classification distribution for hits ---
    cls_distribution: Dict[str, int] = {}
    for h in hits:
        cls = h.get("classification") or "unclassified"
        cls_distribution[cls] = cls_distribution.get(cls, 0) + 1

    # --- 4. Log results ---
    total = len(ground_truth)
    n_hits = len(hits)
    n_misses = len(misses)
    n_skipped = len(skipped)
    hit_rate = (n_hits / (n_hits + n_misses) * 100) if (n_hits + n_misses) > 0 else 0

    logger.info(
        "=== OpenNeuro Classification Benchmark ===\n"
        "  Ground-truth rows:   %d\n"
        "  Skipped (no DOI):    %d\n"
        "  Evaluated:           %d\n"
        "  Hits (in DB):        %d\n"
        "  Misses (not in DB):  %d\n"
        "  Hit rate:            %.1f%%\n"
        "  Classification dist: %s",
        total, n_skipped, n_hits + n_misses,
        n_hits, n_misses, hit_rate,
        json.dumps(cls_distribution, indent=2),
    )

    if misses:
        misses_in_map = [m for m in misses if m.get("in_paper_map")]
        misses_not_in_map = [m for m in misses if not m.get("in_paper_map")]
        logger.info(
            "  Misses in paper_map (primary, not citation): %d\n"
            "  Misses not in DB at all:                     %d",
            len(misses_in_map), len(misses_not_in_map),
        )

    # Log a sample of misses for debugging
    if misses:
        sample = misses[:20]
        logger.info(
            "Sample misses (up to 20):\n%s",
            json.dumps(sample, indent=2, default=str),
        )

    return {
        "total": total,
        "skipped": n_skipped,
        "evaluated": n_hits + n_misses,
        "hits": n_hits,
        "misses": n_misses,
        "hit_rate_pct": round(hit_rate, 1),
        "classification_distribution": cls_distribution,
        "misses_in_paper_map": len([m for m in misses if m.get("in_paper_map")]),
        "misses_not_in_db": len([m for m in misses if not m.get("in_paper_map")]),
    }


dag = DAG(
    "openneuro_classify_benchmark",
    default_args={
        "owner": "neurod3",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="Benchmark classification pipeline against ground-truth OpenNeuro dataset mentions",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["maintenance", "benchmark", "classification"],
)

PythonOperator(
    task_id="run_benchmark",
    python_callable=run_benchmark,
    dag=dag,
)
