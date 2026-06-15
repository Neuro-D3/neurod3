"""
Airflow DAG to fetch and ingest datasets from CRCNS via DataCite.

CRCNS (crcns.org) has no native data API, but every CRCNS dataset is registered
with DataCite under DOI prefix 10.6080. We page through DataCite for the
listing, then resolve each DOI through doi.org to recover the friendly CRCNS
code (e.g. pvc-1, hc-3) and the canonical landing-page URL.
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from utils.database import get_db_connection, create_unified_datasets_view

logger = logging.getLogger(__name__)

DATACITE_URL = "https://api.datacite.org/dois"
DOI_RESOLVER = "https://doi.org"
USER_AGENT = "D3-CRCNS-Ingestion/1.0"

# Matches the friendly CRCNS code in a redirected URL, e.g.
# https://crcns.org/data-sets/vc/pvc-1 -> "pvc-1"
_CRCNS_CODE_RE = re.compile(r"crcns\.org/(?:data-sets/\w+|[\w]+)/([\w]+-\d+)")

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
    'crcns_ingestion',
    default_args=default_args,
    description='Fetch and ingest datasets from CRCNS via DataCite (DOI prefix 10.6080)',
    schedule='@daily',
    catchup=False,
    tags=['datasets', 'crcns', 'ingestion'],
    is_paused_upon_creation=False,
    params={
        'num_datasets': 5000,
        # doi.org is a small shared service; keep concurrency low.
        'enrichment_max_workers': 5,
    },
)


def _parse_iso8601(dt_str: Optional[str]) -> Optional[datetime]:
    if not isinstance(dt_str, str) or not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return datetime.fromisoformat(dt_str)
    except (ValueError, TypeError):
        # DataCite sometimes emits bare years or "YYYY-MM"; try a relaxed parse.
        for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
        logger.warning("Could not parse timestamp '%s'", dt_str)
        return None


def _first_description(attrs: Dict[str, Any]) -> Optional[str]:
    for d in attrs.get("descriptions") or []:
        text = d.get("description") if isinstance(d, dict) else None
        if isinstance(text, str) and text.strip():
            return text.strip()
    return None


def _created_date(attrs: Dict[str, Any]) -> Optional[datetime]:
    for d in attrs.get("dates") or []:
        if isinstance(d, dict) and d.get("dateType") == "Created":
            parsed = _parse_iso8601(d.get("date"))
            if parsed:
                return parsed
    year = attrs.get("publicationYear")
    if isinstance(year, int) and year > 0:
        return datetime(year, 1, 1)
    return None


def _derive_modality(title: Optional[str], description: Optional[str]) -> Optional[str]:
    """Heuristic modality classification.

    Ported from _tmp_find_reuse_friend/archives/crcns.py to keep the same labels
    in use by the existing reuse-analysis pipeline.
    """
    text = ((title or "") + " " + (description or "")).lower()
    if not text.strip():
        return None
    if "electrophysiol" in text or "spike" in text or "extracellular" in text or "tetrode" in text:
        return "ephys"
    if "patch" in text and "clamp" in text:
        return "ephys"
    if "calcium" in text or "fluorescen" in text or "imaging" in text:
        return "imaging"
    if "fmri" in text or "bold" in text:
        return "fmri"
    return "other"


def parse_datacite_record(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize one DataCite record into our row shape (pre-enrichment)."""
    attrs = item.get("attributes") or {}
    doi = attrs.get("doi") or ""
    if not doi:
        return None

    titles = attrs.get("titles") or []
    title = (titles[0].get("title") if titles and isinstance(titles[0], dict) else None) or None
    description = _first_description(attrs)

    creators_raw = attrs.get("creators") or []
    creators = [c.get("name") for c in creators_raw if isinstance(c, dict) and c.get("name")]

    created_at = _created_date(attrs)
    publication_year = attrs.get("publicationYear")

    # At this stage we don't yet have the friendly code; use the DOI so that
    # the upsert in case of an enrichment failure still finds a stable key.
    return {
        "dataset_id": doi,
        "doi": doi,
        "title": title,
        "modality": _derive_modality(title, description),
        "citations": None,
        "papers": None,
        "url": f"{DOI_RESOLVER}/{doi}",
        "description": description,
        "full_description": description,
        "authors": creators or None,
        "contributors": [{"name": c, "roles": []} for c in creators] or None,
        "license": None,
        "num_subjects": None,
        "created_at": created_at,
        "updated_at": None,
        "version": str(publication_year) if publication_year else None,
    }


def create_crcns_table(**context):
    """Create the crcns_dataset table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS crcns_dataset (
        id SERIAL PRIMARY KEY,
        dataset_id VARCHAR(255) NOT NULL UNIQUE,
        doi VARCHAR(255),
        title TEXT,
        modality TEXT,
        citations INTEGER,
        papers INTEGER,
        url TEXT,
        description TEXT,
        full_description TEXT,
        authors JSONB,
        contributors JSONB,
        license TEXT,
        num_subjects INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        version VARCHAR(64)
    );

    CREATE INDEX IF NOT EXISTS idx_crcns_dataset_id ON crcns_dataset(dataset_id);
    CREATE INDEX IF NOT EXISTS idx_crcns_doi ON crcns_dataset(doi);
    CREATE INDEX IF NOT EXISTS idx_crcns_modality ON crcns_dataset(modality);
    CREATE INDEX IF NOT EXISTS idx_crcns_papers ON crcns_dataset(papers DESC);
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                # Tolerate older deployments that pre-date some columns.
                cursor.execute("ALTER TABLE crcns_dataset ADD COLUMN IF NOT EXISTS doi VARCHAR(255);")
                cursor.execute("ALTER TABLE crcns_dataset ADD COLUMN IF NOT EXISTS papers INTEGER;")
                cursor.execute("ALTER TABLE crcns_dataset ADD COLUMN IF NOT EXISTS full_description TEXT;")
                cursor.execute("ALTER TABLE crcns_dataset ADD COLUMN IF NOT EXISTS authors JSONB;")
                cursor.execute("ALTER TABLE crcns_dataset ADD COLUMN IF NOT EXISTS contributors JSONB;")
                cursor.execute("ALTER TABLE crcns_dataset ADD COLUMN IF NOT EXISTS license TEXT;")
                cursor.execute("ALTER TABLE crcns_dataset ADD COLUMN IF NOT EXISTS num_subjects INTEGER;")
                conn.commit()
        logger.info("Successfully created crcns_dataset table (or it already exists)")
    except Exception as e:
        logger.error("Error creating crcns_dataset table: %s", e)
        raise


def fetch_crcns_datasets(**context) -> List[Dict[str, Any]]:
    """Page through DataCite for DOI prefix 10.6080 (CRCNS)."""
    num_datasets = context.get('params', {}).get('num_datasets', 5000)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    datasets: List[Dict[str, Any]] = []
    page = 1
    page_size = 100

    while len(datasets) < num_datasets:
        logger.info(
            "Fetching DataCite page %d (have %d/%d so far)",
            page, len(datasets), num_datasets,
        )
        try:
            resp = session.get(
                DATACITE_URL,
                params={
                    "query": "prefix:10.6080",
                    "page[size]": page_size,
                    "page[number]": page,
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error("DataCite request failed on page %d: %s", page, e)
            raise

        records = payload.get("data") or []
        if not records:
            logger.info("DataCite returned no more records on page %d", page)
            break

        for item in records:
            if len(datasets) >= num_datasets:
                break
            row = parse_datacite_record(item)
            if not row:
                continue
            datasets.append(row)

        total = (payload.get("meta") or {}).get("total", 0)
        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.5)

    logger.info("Fetched %d CRCNS records from DataCite", len(datasets))
    return datasets


def _resolve_doi_to_code(doi: str, session: requests.Session) -> Tuple[Optional[str], Optional[str]]:
    """HEAD doi.org/{doi}, follow redirects, return (crcns_code, final_url)."""
    try:
        resp = session.head(f"{DOI_RESOLVER}/{doi}", allow_redirects=True, timeout=15)
    except requests.RequestException as e:
        logger.debug("HEAD failed for %s: %s", doi, e)
        return None, None

    final_url = resp.url or None
    match = _CRCNS_CODE_RE.search(final_url or "")
    code = match.group(1) if match else None
    return code, final_url


def _enrich_single(ds: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, int]]:
    stats = {"resolved_code": 0, "fallback_doi": 0, "request_errors": 0}
    enriched = ds.copy()
    doi = ds.get("doi")
    if not doi:
        stats["fallback_doi"] = 1
        return enriched, stats

    # requests.Session is not thread-safe; build one per worker call.
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    code, final_url = _resolve_doi_to_code(doi, session)
    if final_url is None:
        stats["request_errors"] = 1
        return enriched, stats

    if code:
        enriched["dataset_id"] = code
        stats["resolved_code"] = 1
    else:
        stats["fallback_doi"] = 1

    enriched["url"] = final_url
    return enriched, stats


def enrich_crcns_current_run(**context) -> List[Dict[str, Any]]:
    """Resolve each DOI to a CRCNS code + canonical landing URL via doi.org."""
    ti = context['ti']
    datasets: List[Dict[str, Any]] = ti.xcom_pull(task_ids='fetch_crcns_datasets') or []
    if not datasets:
        logger.info("No datasets to enrich.")
        return []

    max_workers = context.get('params', {}).get('enrichment_max_workers', 5)
    total = len(datasets)
    logger.info(
        "Resolving DOIs to CRCNS codes for %d datasets (max_workers=%d)",
        total, max_workers,
    )

    results: Dict[int, Tuple[Dict[str, Any], Dict[str, int]]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(_enrich_single, ds): idx
            for idx, ds in enumerate(datasets)
        }
        completed = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            completed += 1
            try:
                results[idx] = future.result()
            except Exception as e:
                logger.error(
                    "Unexpected error enriching %s: %s",
                    datasets[idx].get("doi", "?"), e,
                )
                results[idx] = (datasets[idx], {"request_errors": 1})
            if completed == 1 or completed % 50 == 0 or completed == total:
                logger.info("Enrichment progress: %d/%d", completed, total)

    enriched: List[Dict[str, Any]] = []
    agg = {"resolved_code": 0, "fallback_doi": 0, "request_errors": 0}
    for idx in range(total):
        row, stats = results[idx]
        enriched.append(row)
        for k, v in stats.items():
            agg[k] = agg.get(k, 0) + v

    logger.info(
        "Enrichment complete. total=%d resolved_code=%d fallback_doi=%d request_errors=%d",
        total, agg["resolved_code"], agg["fallback_doi"], agg["request_errors"],
    )

    # Drop duplicates that can arise when two DataCite DOIs resolve to the same
    # CRCNS code (rare but seen for re-issued datasets). Keep the first.
    seen: set = set()
    deduped: List[Dict[str, Any]] = []
    for row in enriched:
        key = row.get("dataset_id")
        if key in seen:
            logger.info("Dropping duplicate dataset_id after enrichment: %s", key)
            continue
        seen.add(key)
        deduped.append(row)
    if len(deduped) < len(enriched):
        logger.info("Deduplicated %d -> %d rows", len(enriched), len(deduped))
    return deduped


def insert_crcns_datasets(**context):
    """Insert or update CRCNS datasets into the database."""
    ti = context['ti']
    datasets = (
        ti.xcom_pull(task_ids='enrich_crcns_current_run')
        or ti.xcom_pull(task_ids='fetch_crcns_datasets')
    )
    if not datasets:
        logger.warning("No datasets to insert")
        return

    insert_sql = """
    INSERT INTO crcns_dataset (
        dataset_id, doi, title, modality, citations, papers, url,
        description, full_description, authors, contributors, license,
        num_subjects, created_at, updated_at, version
    )
    VALUES (
        %(dataset_id)s, %(doi)s, %(title)s, %(modality)s, %(citations)s,
        %(papers)s, %(url)s, %(description)s, %(full_description)s,
        %(authors)s, %(contributors)s, %(license)s, %(num_subjects)s,
        %(created_at)s, %(updated_at)s, %(version)s
    )
    ON CONFLICT (dataset_id) DO UPDATE SET
        doi = COALESCE(EXCLUDED.doi, crcns_dataset.doi),
        title = EXCLUDED.title,
        modality = EXCLUDED.modality,
        citations = EXCLUDED.citations,
        papers = COALESCE(EXCLUDED.papers, crcns_dataset.papers),
        url = EXCLUDED.url,
        description = EXCLUDED.description,
        full_description = COALESCE(EXCLUDED.full_description, crcns_dataset.full_description),
        authors = COALESCE(EXCLUDED.authors, crcns_dataset.authors),
        contributors = COALESCE(EXCLUDED.contributors, crcns_dataset.contributors),
        license = COALESCE(EXCLUDED.license, crcns_dataset.license),
        num_subjects = COALESCE(EXCLUDED.num_subjects, crcns_dataset.num_subjects),
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        version = EXCLUDED.version
    RETURNING (xmax = 0) AS inserted;
    """

    inserted_count = 0
    updated_count = 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for dataset in datasets:
                    row = dict(dataset)
                    authors_val = row.get("authors")
                    row["authors"] = json.dumps(authors_val) if authors_val is not None else None
                    contributors_val = row.get("contributors")
                    row["contributors"] = json.dumps(contributors_val) if contributors_val is not None else None
                    cursor.execute(insert_sql, row)
                    inserted_flag = cursor.fetchone()[0]
                    if inserted_flag:
                        inserted_count += 1
                    else:
                        updated_count += 1
                conn.commit()
        logger.info(
            "Inserted %d new datasets and updated %d existing datasets",
            inserted_count, updated_count,
        )
    except Exception as e:
        logger.error("Error inserting CRCNS datasets: %s", e)
        raise


def create_unified_datasets_view_task(**context):
    """Create or replace the unified_datasets view."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                result = create_unified_datasets_view(cursor)
                conn.commit()
                return result
    except Exception as e:
        logger.error("Error creating/replacing unified_datasets view: %s", e)
        raise


def verify_crcns_data(**context):
    """Verify that CRCNS data was inserted correctly."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM crcns_dataset")
                count = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT modality, COUNT(*) "
                    "FROM crcns_dataset "
                    "GROUP BY modality "
                    "ORDER BY COUNT(*) DESC"
                )
                modality_stats = cursor.fetchall()

                cursor.execute(
                    "SELECT COUNT(*) FROM crcns_dataset WHERE dataset_id LIKE '10.6080/%'"
                )
                unresolved = cursor.fetchone()[0]

        logger.info("Total CRCNS datasets in database: %d", count)
        logger.info("Unresolved (still keyed by DOI): %d", unresolved)
        logger.info("Datasets by modality:")
        for modality, modality_count in modality_stats:
            logger.info("  %s: %d", modality, modality_count)
    except Exception as e:
        logger.error("Error verifying CRCNS data: %s", e)
        raise


create_crcns_table_task = PythonOperator(
    task_id='create_crcns_table',
    python_callable=create_crcns_table,
    dag=dag,
)

fetch_datasets_task = PythonOperator(
    task_id='fetch_crcns_datasets',
    python_callable=fetch_crcns_datasets,
    dag=dag,
)

enrich_datasets_task = PythonOperator(
    task_id='enrich_crcns_current_run',
    python_callable=enrich_crcns_current_run,
    dag=dag,
)

insert_datasets_task = PythonOperator(
    task_id='insert_crcns_datasets',
    python_callable=insert_crcns_datasets,
    dag=dag,
)

create_view_task = PythonOperator(
    task_id='create_unified_datasets_view',
    python_callable=create_unified_datasets_view_task,
    dag=dag,
)

verify_data_task = PythonOperator(
    task_id='verify_crcns_data',
    python_callable=verify_crcns_data,
    dag=dag,
)

create_crcns_table_task >> fetch_datasets_task >> enrich_datasets_task >> insert_datasets_task >> create_view_task >> verify_data_task
