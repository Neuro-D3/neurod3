"""
Airflow DAG to fetch and ingest datasets from DANDI Archive API.
This DAG fetches datasets from the DANDI API and stores them in PostgreSQL.
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.database import get_db_connection

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'neurod3',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dandi_ingestion',
    default_args=default_args,
    description='Fetch and ingest datasets from DANDI Archive API',
    schedule_interval='@daily',
    catchup=False,
    tags=['datasets', 'dandi', 'ingestion'],
    is_paused_upon_creation=False,
    params={
        'num_datasets': 5000,  # Default number of datasets to fetch
    },
)


def _parse_iso8601(dt_str: str) -> Optional[datetime]:
    """Parse ISO8601 timestamps coming back from DANDI, return None on failure."""
    if not isinstance(dt_str, str):
        return None
    try:
        if dt_str.endswith("Z"):
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return datetime.fromisoformat(dt_str)
    except (ValueError, TypeError):
        logger.warning("Could not parse timestamp '%s'", dt_str)
        return None


def parse_dandiset(dandiset: Dict[str, Any]) -> Dict[str, Any]:
    dataset_id = (
        str(dandiset.get("identifier", ""))
        if dandiset.get("identifier") is not None
        else ""
    )

    created_at = _parse_iso8601(dandiset.get("created"))
    root_modified = _parse_iso8601(dandiset.get("modified"))

    mrpv = dandiset.get("most_recent_published_version")
    draft_v = dandiset.get("draft_version")

    version_obj = mrpv if isinstance(mrpv, dict) else None
    if version_obj is None and isinstance(draft_v, dict):
        version_obj = draft_v

    title: Optional[str] = None
    updated_at: Optional[datetime] = None
    url: str = ""
    version_id: Optional[str] = None

    if version_obj:
        title = version_obj.get("name") or None
        updated_at = _parse_iso8601(version_obj.get("modified")) or root_modified
        version_id = version_obj.get("version") or None

        if dataset_id:
            if version_id:
                url = f"https://dandiarchive.org/dandiset/{dataset_id}/{version_id}"
            else:
                url = f"https://dandiarchive.org/dandiset/{dataset_id}"
    else:
        title = None
        updated_at = root_modified
        if dataset_id:
            url = f"https://dandiarchive.org/dandiset/{dataset_id}"

    # list endpoint: no description, no modalities — fill these later
    description: Optional[str] = None
    modality: Optional[str] = None

    citations = random.randint(1, 1000)

    return {
        "dataset_id": dataset_id,
        "title": title,
        "modality": modality,
        "citations": citations,
        "url": url,
        "description": description,
        "created_at": created_at,
        "updated_at": updated_at,
        "version": version_id,  # <-- now tracked explicitly
    }



def fetch_dandi_datasets(**context) -> List[Dict[str, Any]]:
    """Fetch datasets from DANDI API and normalize them (no description yet)."""
    num_datasets = context.get('params', {}).get('num_datasets', 50)

    dandi_api_url = "https://api.dandiarchive.org/api/dandisets/"
    datasets: List[Dict[str, Any]] = []
    next_url: Optional[str] = dandi_api_url

    try:
        while len(datasets) < num_datasets and next_url:
            logger.info(
                "Fetching DANDI datasets: %d/%d fetched so far",
                len(datasets),
                num_datasets,
            )
            response = requests.get(next_url, timeout=30)
            response.raise_for_status()

            data = response.json()

            # DANDI API returns results in 'results' field
            results = data.get("results") or []
            if not results:
                logger.info("No more datasets available from DANDI API")
                break

            for dandiset in results:
                if len(datasets) >= num_datasets:
                    break

                dataset = parse_dandiset(dandiset)

                if not dataset["dataset_id"]:
                    logger.warning(
                        "Skipping dandiset with missing identifier: %s", dandiset
                    )
                    continue

                datasets.append(dataset)
                logger.info(
                    "Fetched dataset: %s - %s",
                    dataset["dataset_id"],
                    dataset["title"],
                )

            next_url = data.get("next")

        logger.info("Successfully fetched %d datasets from DANDI API", len(datasets))
        return datasets

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching datasets from DANDI API: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error while fetching DANDI datasets: %s", e)
        raise


def _enrich_single_dataset(ds: Dict[str, Any], api_base: str) -> tuple:
    """
    Enrich a single dataset by fetching its metadata from DANDI API.
    Returns a tuple of (enriched_dataset, stats_dict).
    """
    dataset_id = ds.get("dataset_id")
    version = ds.get("version") or "draft"
    
    # Create a copy to avoid modifying the original
    enriched_ds = ds.copy()
    stats = {
        "enriched_desc": 0,
        "enriched_modality": 0,
        "skipped_no_id": 0,
        "skipped_no_metadata": 0,
        "skipped_no_description": 0,
        "request_errors": 0,
    }

    if not dataset_id:
        stats["skipped_no_id"] = 1
        return enriched_ds, stats

    meta_url = f"{api_base}/dandisets/{dataset_id}/versions/{version}/"

    try:
        resp = requests.get(meta_url, timeout=30)
        if resp.status_code == 404 and version != "draft":
            logger.debug(
                "Version %s not found for %s, falling back to 'draft'",
                version,
                dataset_id,
            )
            meta_url = f"{api_base}/dandisets/{dataset_id}/versions/draft/"
            resp = requests.get(meta_url, timeout=30)

        if resp.status_code == 404:
            logger.warning(
                "Metadata not found for %s@%s (404 even on draft). Skipping enrichment.",
                dataset_id,
                version,
            )
            stats["skipped_no_metadata"] = 1
            return enriched_ds, stats

        resp.raise_for_status()
        v_json = resp.json()
    except requests.RequestException as e:
        stats["request_errors"] = 1
        logger.warning(
            "Failed to fetch metadata for %s@%s: %s",
            dataset_id,
            version,
            e,
        )
        return enriched_ds, stats

    # DANDI now has description sometimes under metadata, sometimes top-level
    meta = v_json.get("metadata") or {}

    # --- description ---
    description = meta.get("description") or v_json.get("description")
    if isinstance(description, dict):
        description = (
            description.get("value")
            or description.get("@value")
            or None
        )

    if description and isinstance(description, str):
        enriched_ds["description"] = description
        stats["enriched_desc"] = 1
    else:
        stats["skipped_no_description"] = 1

    # --- modality: use TOP-LEVEL assetsSummary, not metadata.assetsSummary ---
    modality_str: Optional[str] = None

    assets_summary = v_json.get("assetsSummary")
    if isinstance(assets_summary, dict):
        # Modality-like info is usually here
        candidate_keys = [
            "modalities",
            "modality",
            "approach",
            "measurementTechnique",
            "dataType",
            "dataTypes",
        ]
        parts: List[str] = []
        for key in candidate_keys:
            items = assets_summary.get(key)
            if not items:
                continue
            if not isinstance(items, list):
                items = [items]
            for item in items:
                name = None
                if isinstance(item, dict):
                    name = (
                        item.get("name")
                        or item.get("label")
                        or item.get("identifier")
                    )
                elif isinstance(item, str):
                    name = item
                if name:
                    parts.append(name)

        parts = [p.strip() for p in parts if p and p.strip()]
        if parts:
            modality_str = ", ".join(sorted(set(parts)))

    # Fallback: if someone stuffed modalities directly into metadata
    if not modality_str:
        modalities = meta.get("modalities") or v_json.get("modalities")
        if isinstance(modalities, list):
            flat: List[str] = []
            for m in modalities:
                if isinstance(m, dict):
                    flat.append(
                        m.get("name")
                        or m.get("identifier")
                        or m.get("label")
                        or str(m)
                    )
                else:
                    flat.append(str(m))
            flat = [p.strip() for p in flat if p and p.strip()]
            if flat:
                modality_str = ", ".join(sorted(set(flat)))
        elif isinstance(modalities, str):
            modality_str = modalities.strip() or None

    if modality_str:
        enriched_ds["modality"] = modality_str
        stats["enriched_modality"] = 1

    return enriched_ds, stats


def enrich_dandi_current_run(**context) -> List[Dict[str, Any]]:
    """
    For the current run's datasets (from fetch_dandi_datasets), call the
    dandiset version metadata endpoint and populate description + modality
    in memory using concurrent requests.

      GET /api/dandisets/{dataset_id}/versions/{version}/

    We look for:
      - metadata.description  (fallback to top-level description)
      - top-level assetsSummary.* (approach / measurementTechnique / dataType / modalities)
        -> modality column
    
    Uses ThreadPoolExecutor for concurrent HTTP requests to improve performance.
    """
    ti = context["ti"]
    datasets: List[Dict[str, Any]] = ti.xcom_pull(task_ids="fetch_dandi_datasets") or []
    api_base = "https://api.dandiarchive.org/api"
    
    # Get max_workers from DAG params or use default
    max_workers = context.get('params', {}).get('enrichment_max_workers', 20)

    total = len(datasets)
    if not datasets:
        logger.info("No datasets from current run to enrich.")
        return []

    logger.info("Starting concurrent description/modality enrichment for %d datasets (max_workers=%d)", total, max_workers)

    enriched: List[Dict[str, Any]] = []
    enriched_desc = 0
    enriched_modality = 0
    skipped_no_id = 0
    skipped_no_metadata = 0
    skipped_no_description = 0
    request_errors = 0

    # Use ThreadPoolExecutor for concurrent requests
    # Store results with their original index to maintain order
    results: Dict[int, tuple] = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_index = {
            executor.submit(_enrich_single_dataset, ds, api_base): idx
            for idx, ds in enumerate(datasets)
        }
        
        # Process completed tasks as they finish
        completed = 0
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            original_ds = datasets[idx]
            completed += 1
            
            try:
                enriched_ds, stats = future.result()
                results[idx] = (enriched_ds, stats)
                
            except Exception as e:
                logger.error(
                    "Error processing dataset %s (index %d): %s",
                    original_ds.get("dataset_id", "unknown"),
                    idx,
                    e,
                )
                # Store original dataset on error with error stats
                results[idx] = (original_ds, {"request_errors": 1})
            
            # Progress log
            if completed == 1 or completed % 50 == 0 or completed == total:
                logger.info(
                    "Enrichment progress: %d/%d (%.1f%%)",
                    completed,
                    total,
                    (completed / total) * 100,
                )
    
    # Process results in original order and aggregate statistics
    for idx in range(total):
        enriched_ds, stats = results[idx]
        enriched.append(enriched_ds)
        
        # Aggregate statistics
        enriched_desc += stats.get("enriched_desc", 0)
        enriched_modality += stats.get("enriched_modality", 0)
        skipped_no_id += stats.get("skipped_no_id", 0)
        skipped_no_metadata += stats.get("skipped_no_metadata", 0)
        skipped_no_description += stats.get("skipped_no_description", 0)
        request_errors += stats.get("request_errors", 0)

    logger.info(
        "Enrichment complete. Total=%d, "
        "desc_enriched=%d, modality_enriched=%d, "
        "skipped_no_id=%d, skipped_no_metadata=%d, "
        "skipped_no_description=%d, request_errors=%d",
        total,
        enriched_desc,
        enriched_modality,
        skipped_no_id,
        skipped_no_metadata,
        skipped_no_description,
        request_errors,
    )

    return enriched




def insert_dandi_datasets(**context):
    """Insert or update DANDI datasets into the database."""
    ti = context['ti']
    datasets = (
        ti.xcom_pull(task_ids='enrich_dandi_current_run')
        or ti.xcom_pull(task_ids='fetch_dandi_datasets')
    )

    if not datasets:
        logger.warning("No datasets to insert")
        return

    insert_sql = """
    INSERT INTO dandi_dataset (dataset_id, title, modality, citations, url, description, created_at, updated_at, version)
    VALUES (%(dataset_id)s, %(title)s, %(modality)s, %(citations)s, %(url)s, %(description)s, %(created_at)s, %(updated_at)s, %(version)s)
    ON CONFLICT (dataset_id)
    DO UPDATE SET
        title = EXCLUDED.title,
        modality = EXCLUDED.modality,
        citations = EXCLUDED.citations,
        url = EXCLUDED.url,
        description = EXCLUDED.description,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        version = EXCLUDED.version
    RETURNING (xmax = 0) AS inserted;
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                inserted_count = 0
                updated_count = 0

                for dataset in datasets:

                    cursor.execute(insert_sql, dataset)
                    # Use RETURNING inserted flag to distinguish insert vs update without a pre-check
                    inserted_flag = cursor.fetchone()[0]
                    if inserted_flag:
                        inserted_count += 1
                    else:
                        updated_count += 1

                conn.commit()

        logger.info(
            "Successfully inserted %d new datasets and updated %d existing datasets",
            inserted_count,
            updated_count,
        )
    except Exception as e:
        logger.error("Error inserting DANDI datasets: %s", e)
        raise



def verify_dandi_data(**context):
    """Verify that DANDI data was inserted correctly."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM dandi_dataset")
                count = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT modality, COUNT(*) "
                    "FROM dandi_dataset "
                    "GROUP BY modality "
                    "ORDER BY COUNT(*) DESC"
                )
                modality_stats = cursor.fetchall()

                cursor.execute("SELECT SUM(citations) FROM dandi_dataset")
                total_citations = cursor.fetchone()[0] or 0

        logger.info("Total DANDI datasets in database: %d", count)
        logger.info("Total citations: %d", total_citations)
        logger.info("Datasets by modality:")
        for modality, modality_count in modality_stats:
            logger.info("  %s: %d", modality, modality_count)

    except Exception as e:
        logger.error("Error verifying DANDI data: %s", e)
        raise


# Define tasks
fetch_datasets_task = PythonOperator(
    task_id='fetch_dandi_datasets',
    python_callable=fetch_dandi_datasets,
    dag=dag,
)

enrich_dandi_data_task = PythonOperator(
    task_id='enrich_dandi_current_run',
    python_callable=enrich_dandi_current_run,
    dag=dag,
)

insert_datasets_task = PythonOperator(
    task_id='insert_dandi_datasets',
    python_callable=insert_dandi_datasets,
    dag=dag,
)

verify_data_task = PythonOperator(
    task_id='verify_dandi_data',
    python_callable=verify_dandi_data,
    dag=dag,
)

# Set task dependencies:
# fetch -> enrich (current run only) -> insert -> verify
fetch_datasets_task >> enrich_dandi_data_task >> insert_datasets_task >> verify_data_task
