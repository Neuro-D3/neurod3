"""
Airflow DAG to fetch and ingest datasets from SPARC via the Pennsieve Discover API.

SPARC (sparc.science) publishes its catalog through Pennsieve. Unlike CRCNS (which
requires HTML scraping for paper DOIs) and DANDI (custom REST), SPARC's records are
already richly structured: `name`, `description`, `firstPublishedAt`, `doi`,
`contributors`, `size`, `fileCount`, `tags`, `license`, and `modelCount` (subject
counts) all come back in the list response, so the enrichment pass only adds
modality and a few optional fields.
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from utils.database import get_db_connection, create_unified_datasets_view

logger = logging.getLogger(__name__)

PENNSIEVE_API = "https://api.pennsieve.io/discover"
SPARC_ORG_ID = 367
USER_AGENT = "D3-SPARC-Ingestion/1.0"

# Pennsieve marks pre-publication / test entries with one of these tags; skip.
_SKIP_TAGS = {"test", "embargo", "testing"}

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
    'sparc_ingestion',
    default_args=default_args,
    description='Fetch and ingest datasets from SPARC via Pennsieve Discover API',
    schedule='@daily',
    catchup=False,
    tags=['datasets', 'sparc', 'ingestion'],
    is_paused_upon_creation=False,
    params={
        'num_datasets': 5000,
        'enrichment_max_workers': 5,
    },
)


def _parse_pennsieve_date(value: Optional[str]) -> Optional[datetime]:
    """Pennsieve emits dates as 'MM/DD/YYYY HH:MM:SS' or sometimes ISO8601."""
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    # Try ISO first.
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        pass
    # Pennsieve's "07/03/2019 18:14:25" format.
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    logger.warning("Could not parse Pennsieve timestamp %r", s)
    return None


def _derive_modality(title: Optional[str], description: Optional[str]) -> Optional[str]:
    """Heuristic — same shape as CRCNS, slightly broader since SPARC covers
    a wider modality range (autonomic, peripheral, anatomy, etc.)."""
    text = ((title or "") + " " + (description or "")).lower()
    if not text.strip():
        return None
    if "electrophysiol" in text or "spike" in text or "extracellular" in text or "tetrode" in text:
        return "ephys"
    if "patch" in text and "clamp" in text:
        return "ephys"
    if "calcium" in text or "fluorescen" in text or "two-photon" in text or "two photon" in text:
        return "imaging"
    if "fmri" in text or "bold" in text:
        return "fmri"
    if "mri" in text or "magnetic resonance" in text:
        return "mri"
    if "microct" in text or "micro-ct" in text or "histolog" in text or "anatom" in text:
        return "anatomy"
    return "other"


def _subject_count(model_count: Any) -> Optional[int]:
    """Sum entries in Pennsieve's modelCount array whose modelName looks like a
    per-subject count. Case-insensitive ('Subject' vs 'subject' vs 'animal_subject')."""
    if not isinstance(model_count, list):
        return None
    total = 0
    matched = False
    for entry in model_count:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("modelName") or "").strip().lower()
        if name in ("subject", "animal_subject", "animalsubject", "human_subject", "humansubject"):
            try:
                total += int(entry.get("count") or 0)
                matched = True
            except (TypeError, ValueError):
                continue
    return total if matched else None


def _format_contributors(raw: Any) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Pennsieve returns [{firstName, lastName, orcid, ...}, ...]. Return
    (authors_list_of_names, contributors_records_with_roles_for_jsonb)."""
    if not isinstance(raw, list):
        return [], []
    authors: List[str] = []
    records: List[Dict[str, Any]] = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        first = (c.get("firstName") or "").strip()
        last = (c.get("lastName") or "").strip()
        if last and first:
            name = f"{last}, {first}"
        elif last or first:
            name = (last or first).strip()
        else:
            continue
        authors.append(name)
        rec: Dict[str, Any] = {"name": name, "roles": []}
        if c.get("orcid"):
            rec["orcid"] = c.get("orcid")
        records.append(rec)
    return authors, records


def parse_sparc_dataset(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize one Pennsieve record into our row shape."""
    raw_id = record.get("id")
    if raw_id is None:
        return None
    dataset_id = str(raw_id)

    # The Pennsieve Discover `/datasets` endpoint ignores the `organizationId` query
    # param, so it returns the entire public catalog (Mayo, IT'IS, PennEPI, ...), not
    # just SPARC. Filter to the SPARC Consortium org here, and to research datasets so
    # cross-portal "collection" entries (e.g. PennEPI id 590, which redirects off
    # sparc.science to epilepsy.science) don't get ingested as SPARC.
    if record.get("organizationId") != SPARC_ORG_ID:
        return None
    dataset_type = (record.get("datasetType") or "").strip().lower()
    if dataset_type and dataset_type != "research":
        return None

    tags = {(t or "").strip().lower() for t in (record.get("tags") or [])}
    if tags & _SKIP_TAGS:
        return None

    title = record.get("name") or None
    description = record.get("description") or None
    doi = record.get("doi") or None
    license_val = record.get("license") or None
    n_files = record.get("fileCount")
    n_files = int(n_files) if isinstance(n_files, int) else None
    n_subjects = _subject_count(record.get("modelCount"))

    created_at = _parse_pennsieve_date(record.get("firstPublishedAt")) or _parse_pennsieve_date(record.get("createdAt"))
    updated_at = _parse_pennsieve_date(record.get("revisedAt")) or _parse_pennsieve_date(record.get("updatedAt"))

    authors, contributors = _format_contributors(record.get("contributors"))
    version_val = record.get("version")
    version_str = str(version_val) if version_val is not None else None

    return {
        "dataset_id": dataset_id,
        "doi": doi,
        "title": title,
        "modality": _derive_modality(title, description),
        # papers / citations are populated by the (deferred) sparc_paper_mapping DAG.
        "citations": None,
        "papers": None,
        "url": f"https://sparc.science/datasets/{dataset_id}",
        "description": description,
        "full_description": description,
        "authors": authors or None,
        "contributors": contributors or None,
        "license": license_val,
        "num_subjects": n_subjects,
        "n_files": n_files,
        "created_at": created_at,
        "updated_at": updated_at,
        "version": version_str,
    }


def create_sparc_table(**context):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sparc_dataset (
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
        n_files INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        version VARCHAR(64)
    );

    CREATE INDEX IF NOT EXISTS idx_sparc_dataset_id ON sparc_dataset(dataset_id);
    CREATE INDEX IF NOT EXISTS idx_sparc_doi ON sparc_dataset(doi);
    CREATE INDEX IF NOT EXISTS idx_sparc_modality ON sparc_dataset(modality);
    CREATE INDEX IF NOT EXISTS idx_sparc_papers ON sparc_dataset(papers DESC);
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                # Tolerate older deployments — additive columns only.
                cursor.execute("ALTER TABLE sparc_dataset ADD COLUMN IF NOT EXISTS doi VARCHAR(255);")
                cursor.execute("ALTER TABLE sparc_dataset ADD COLUMN IF NOT EXISTS papers INTEGER;")
                cursor.execute("ALTER TABLE sparc_dataset ADD COLUMN IF NOT EXISTS full_description TEXT;")
                cursor.execute("ALTER TABLE sparc_dataset ADD COLUMN IF NOT EXISTS authors JSONB;")
                cursor.execute("ALTER TABLE sparc_dataset ADD COLUMN IF NOT EXISTS contributors JSONB;")
                cursor.execute("ALTER TABLE sparc_dataset ADD COLUMN IF NOT EXISTS license TEXT;")
                cursor.execute("ALTER TABLE sparc_dataset ADD COLUMN IF NOT EXISTS num_subjects INTEGER;")
                cursor.execute("ALTER TABLE sparc_dataset ADD COLUMN IF NOT EXISTS n_files INTEGER;")
                conn.commit()
        logger.info("Successfully created sparc_dataset table (or it already exists)")
    except Exception as e:
        logger.error("Error creating sparc_dataset table: %s", e)
        raise


def fetch_sparc_datasets(**context) -> List[Dict[str, Any]]:
    num_datasets = context.get('params', {}).get('num_datasets', 5000)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    datasets: List[Dict[str, Any]] = []
    offset = 0
    page_size = 100

    try:
        while len(datasets) < num_datasets:
            logger.info(
                "Fetching Pennsieve page (offset=%d): %d/%d collected",
                offset, len(datasets), num_datasets,
            )
            resp = session.get(
                f"{PENNSIEVE_API}/datasets",
                params={
                    "limit": page_size,
                    "offset": offset,
                    "organizationId": SPARC_ORG_ID,
                    "orderBy": "date",
                    "orderDirection": "asc",
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()

            records = payload.get("datasets") or []
            if not records:
                logger.info("Pennsieve returned no more datasets at offset %d", offset)
                break

            for rec in records:
                if len(datasets) >= num_datasets:
                    break
                row = parse_sparc_dataset(rec)
                if row is None:
                    continue
                datasets.append(row)

            total = int(payload.get("totalCount") or 0)
            offset += page_size
            if offset >= total:
                break
            time.sleep(0.2)
    except requests.exceptions.RequestException as e:
        logger.error("Pennsieve request failed: %s", e)
        raise

    logger.info("Fetched %d SPARC datasets from Pennsieve", len(datasets))
    return datasets


def _enrich_single(ds: Dict[str, Any], session: requests.Session) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """Hit /discover/datasets/{id} for richer fields if the list payload was thin.

    The list endpoint already includes most fields we need, so this is best-effort:
    we only overwrite when the detail endpoint returns a better value, and we tolerate
    failures silently.
    """
    stats = {"enriched": 0, "request_errors": 0}
    enriched = dict(ds)
    ds_id = ds.get("dataset_id")
    if not ds_id:
        return enriched, stats

    try:
        resp = session.get(
            f"{PENNSIEVE_API}/datasets/{ds_id}",
            timeout=20,
            headers={"User-Agent": USER_AGENT},
        )
        if resp.status_code != 200:
            return enriched, stats
        detail = resp.json()
    except requests.RequestException:
        stats["request_errors"] = 1
        return enriched, stats

    if not enriched.get("num_subjects"):
        n_sub = _subject_count(detail.get("modelCount"))
        if n_sub:
            enriched["num_subjects"] = n_sub
            stats["enriched"] = 1

    # Some detail responses carry a longer readme; use it for full_description.
    readme = detail.get("readme")
    if isinstance(readme, str) and readme.strip() and (
        not enriched.get("full_description")
        or len(readme) > len(enriched.get("full_description") or "")
    ):
        enriched["full_description"] = readme
        stats["enriched"] = 1

    return enriched, stats


def enrich_sparc_current_run(**context) -> List[Dict[str, Any]]:
    ti = context['ti']
    datasets: List[Dict[str, Any]] = ti.xcom_pull(task_ids='fetch_sparc_datasets') or []
    if not datasets:
        logger.info("No SPARC datasets to enrich.")
        return []

    max_workers = context.get('params', {}).get('enrichment_max_workers', 5)
    total = len(datasets)
    logger.info("Enriching %d SPARC datasets (max_workers=%d)", total, max_workers)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    results: Dict[int, Tuple[Dict[str, Any], Dict[str, int]]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {executor.submit(_enrich_single, ds, session): i for i, ds in enumerate(datasets)}
        completed = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            completed += 1
            try:
                results[idx] = future.result()
            except Exception as e:
                logger.error("Enrichment error for %s: %s", datasets[idx].get("dataset_id"), e)
                results[idx] = (datasets[idx], {"request_errors": 1})
            if completed == 1 or completed % 50 == 0 or completed == total:
                logger.info("Enrichment progress: %d/%d", completed, total)

    enriched_list: List[Dict[str, Any]] = []
    agg = {"enriched": 0, "request_errors": 0}
    for idx in range(total):
        row, stats = results[idx]
        enriched_list.append(row)
        for k, v in stats.items():
            agg[k] = agg.get(k, 0) + v

    logger.info(
        "SPARC enrichment complete. total=%d enriched=%d request_errors=%d",
        total, agg["enriched"], agg["request_errors"],
    )
    return enriched_list


def insert_sparc_datasets(**context):
    ti = context['ti']
    datasets = (
        ti.xcom_pull(task_ids='enrich_sparc_current_run')
        or ti.xcom_pull(task_ids='fetch_sparc_datasets')
    )
    if not datasets:
        logger.warning("No SPARC datasets to insert")
        return

    insert_sql = """
    INSERT INTO sparc_dataset (
        dataset_id, doi, title, modality, citations, papers, url,
        description, full_description, authors, contributors, license,
        num_subjects, n_files, created_at, updated_at, version
    )
    VALUES (
        %(dataset_id)s, %(doi)s, %(title)s, %(modality)s, %(citations)s,
        %(papers)s, %(url)s, %(description)s, %(full_description)s,
        %(authors)s, %(contributors)s, %(license)s, %(num_subjects)s,
        %(n_files)s, %(created_at)s, %(updated_at)s, %(version)s
    )
    ON CONFLICT (dataset_id) DO UPDATE SET
        doi = COALESCE(EXCLUDED.doi, sparc_dataset.doi),
        title = EXCLUDED.title,
        modality = EXCLUDED.modality,
        citations = EXCLUDED.citations,
        papers = COALESCE(EXCLUDED.papers, sparc_dataset.papers),
        url = EXCLUDED.url,
        description = EXCLUDED.description,
        full_description = COALESCE(EXCLUDED.full_description, sparc_dataset.full_description),
        authors = COALESCE(EXCLUDED.authors, sparc_dataset.authors),
        contributors = COALESCE(EXCLUDED.contributors, sparc_dataset.contributors),
        license = COALESCE(EXCLUDED.license, sparc_dataset.license),
        num_subjects = COALESCE(EXCLUDED.num_subjects, sparc_dataset.num_subjects),
        n_files = COALESCE(EXCLUDED.n_files, sparc_dataset.n_files),
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
            "Inserted %d new SPARC datasets and updated %d existing datasets",
            inserted_count, updated_count,
        )
    except Exception as e:
        logger.error("Error inserting SPARC datasets: %s", e)
        raise


def create_unified_datasets_view_task(**context):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                result = create_unified_datasets_view(cursor)
                conn.commit()
                return result
    except Exception as e:
        logger.error("Error creating/replacing unified_datasets view: %s", e)
        raise


def verify_sparc_data(**context):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM sparc_dataset")
                count = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT modality, COUNT(*) FROM sparc_dataset "
                    "GROUP BY modality ORDER BY COUNT(*) DESC"
                )
                modality_stats = cursor.fetchall()

                cursor.execute(
                    "SELECT COUNT(*) FROM sparc_dataset WHERE num_subjects IS NOT NULL"
                )
                with_subjects = cursor.fetchone()[0]

        logger.info("Total SPARC datasets in database: %d", count)
        logger.info("Datasets with subject counts: %d", with_subjects)
        logger.info("Datasets by modality:")
        for modality, modality_count in modality_stats:
            logger.info("  %s: %d", modality, modality_count)
    except Exception as e:
        logger.error("Error verifying SPARC data: %s", e)
        raise


create_sparc_table_task = PythonOperator(
    task_id='create_sparc_table',
    python_callable=create_sparc_table,
    dag=dag,
)

fetch_datasets_task = PythonOperator(
    task_id='fetch_sparc_datasets',
    python_callable=fetch_sparc_datasets,
    dag=dag,
)

enrich_datasets_task = PythonOperator(
    task_id='enrich_sparc_current_run',
    python_callable=enrich_sparc_current_run,
    dag=dag,
)

insert_datasets_task = PythonOperator(
    task_id='insert_sparc_datasets',
    python_callable=insert_sparc_datasets,
    dag=dag,
)

create_view_task = PythonOperator(
    task_id='create_unified_datasets_view',
    python_callable=create_unified_datasets_view_task,
    dag=dag,
)

verify_data_task = PythonOperator(
    task_id='verify_sparc_data',
    python_callable=verify_sparc_data,
    dag=dag,
)

create_sparc_table_task >> fetch_datasets_task >> enrich_datasets_task >> insert_datasets_task >> create_view_task >> verify_data_task
