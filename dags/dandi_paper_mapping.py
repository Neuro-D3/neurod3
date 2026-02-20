"""
Airflow DAG: Resolve DANDI dataset -> paper DOI/title mappings.

This DAG runs after `dandi_ingestion` and:
- loads candidate DANDI datasets from Postgres
- filters out test/dummy datasets
- resolves associated paper DOIs from DANDI metadata (relatedResource) and description text
- resolves paper titles/IDs via external scholarly APIs
- persists normalized mappings in Postgres
- writes per-run artifacts to disk for later cloud storage handoff

By default, it only processes 50 datasets per run to validate behavior.

Production-scale notes:
- To process ALL *unmapped* DANDI datasets, set `max_datasets_per_run` to "all" (or 0/None) in DAG params.
- This DAG uses Airflow dynamic task mapping at *batch* granularity.
- To limit parallel API pressure, configure an Airflow pool named `dandi_paper_api_pool` with a small slot count
  (e.g., 1–3) and keep the mapped task assigned to that pool.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

from airflow import DAG

try:
    # Airflow 3+ preferred import
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:  # pragma: no cover
    from airflow.operators.python import PythonOperator  # type: ignore

#
# NOTE: This DAG is designed to run immediately when triggered (manual or scheduled).
# We intentionally do NOT block on `dandi_ingestion` finishing; instead we read whatever
# is currently present in Postgres.
#

from utils.database import get_db_connection
from utils.cache_keys import paper_cache_key_for_doi
from utils.find_reuse_core import normalize_doi, Telemetry
from utils.paper_fulltext import fetch_fulltext_oa
from utils.paper_resolution import (
    PaperResolutionResult,
    resolve_papers_for_dandiset,
)

try:
    # Needed for dynamic task mapping with classic operators
    from airflow.models.xcom_arg import XComArg
except Exception:  # pragma: no cover
    XComArg = None  # type: ignore

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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_run_id(run_id: str) -> str:
    # Airflow run_id can contain ":" and other separators; make it path-safe.
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("_")


def _get_output_root() -> Path:
    # Keep output under the DAGs volume by default (works in docker-compose).
    # This will eventually be replaced by cloud storage; keep a single env var switch.
    env = os.getenv("DANDI_PAPER_MAPPING_OUTPUT_DIR", "").strip()
    if env:
        return Path(env)
    return Path(__file__).parent / "output" / "dandi_paper_mapping"


def _parse_max_datasets_per_run(value: Any) -> Optional[int]:
    """
    Interpret max_datasets_per_run from DAG params.

    Accepted "no cap" values:
    - None / missing
    - 0
    - "all" (case-insensitive)

    Returns:
    - None for no cap
    - int > 0 otherwise
    """
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"", "all", "none", "null"}:
            return None
        try:
            value = int(v)
        except Exception:
            raise ValueError(f"Invalid max_datasets_per_run: {value!r}")
    if isinstance(value, bool):
        raise ValueError(f"Invalid max_datasets_per_run: {value!r}")
    try:
        n = int(value)
    except Exception:
        raise ValueError(f"Invalid max_datasets_per_run: {value!r}")
    if n <= 0:
        return None
    return n


def _parse_batch_size(value: Any, default: int = 25) -> int:
    if value is None:
        return default
    if isinstance(value, str) and value.strip():
        value = int(value.strip())
    n = int(value)
    if n <= 0:
        raise ValueError(f"Invalid batch_size: {value!r}")
    # Safety cap: too many per batch makes tasks very long and retries painful.
    return min(n, 200)


# ---------------------------
# Filtering (test/dummy)
# ---------------------------

_TEST_DUMMY_KEYWORDS = (
    "test",
    "dummy",
    "example",
    "sample",
    "tutorial",
    "benchmark",
    "synthetic",
    "placeholder",
)


def is_test_or_dummy_dataset(title: Optional[str], description: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Heuristic filter for non-production datasets.
    Returns (is_filtered, reason).
    """
    hay = " ".join([title or "", description or ""]).strip().lower()
    if not hay:
        return False, None
    for kw in _TEST_DUMMY_KEYWORDS:
        if re.search(rf"\\b{re.escape(kw)}\\b", hay):
            return True, f"keyword:{kw}"
    return False, None


def keyword_filter_dataset(
    title: Optional[str], description: Optional[str], keywords: Tuple[str, ...]
) -> Tuple[bool, Optional[str]]:
    """
    Same as is_test_or_dummy_dataset, but with runtime-supplied keywords.
    """
    hay = " ".join([title or "", description or ""]).strip().lower()
    if not hay:
        return False, None
    for kw in keywords:
        if re.search(rf"\\b{re.escape(kw)}\\b", hay):
            return True, f"keyword:{kw}"
    return False, None


# ---------------------------
# DB schema + persistence
# ---------------------------

def create_paper_mapping_tables(**context) -> None:
    """
    Create the tables/views used by this DAG.
    Keep SQL static (no identifier interpolation) to avoid injection risks.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS dandi_paper_resolution_runs (
        run_id TEXT PRIMARY KEY,
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        max_datasets_per_run INTEGER,
        candidates_loaded INTEGER,
        candidates_processed INTEGER,
        filtered_out INTEGER,
        resolved_mappings INTEGER,
        unresolved_datasets INTEGER,
        api_429_count INTEGER,
        api_5xx_count INTEGER,
        api_retry_count INTEGER,
        api_throttled_sleep_seconds DOUBLE PRECISION,
        output_path TEXT,
        summary JSONB
    );

    CREATE TABLE IF NOT EXISTS papers (
        paper_doi TEXT PRIMARY KEY,
        openalex_id TEXT,
        title TEXT,
        authors JSONB,
        fulltext_cache_key TEXT,
        fulltext_cached_at TIMESTAMPTZ,
        fulltext_source TEXT,
        fulltext_available BOOLEAN,
        fulltext_reason TEXT,
        source TEXT,
        fetched_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Allow schema evolution
    ALTER TABLE papers ADD COLUMN IF NOT EXISTS authors JSONB;
    ALTER TABLE papers ADD COLUMN IF NOT EXISTS fulltext_cache_key TEXT;
    ALTER TABLE papers ADD COLUMN IF NOT EXISTS fulltext_cached_at TIMESTAMPTZ;
    ALTER TABLE papers ADD COLUMN IF NOT EXISTS fulltext_source TEXT;
    ALTER TABLE papers ADD COLUMN IF NOT EXISTS fulltext_available BOOLEAN;
    ALTER TABLE papers ADD COLUMN IF NOT EXISTS fulltext_reason TEXT;

    CREATE TABLE IF NOT EXISTS dandi_paper_map (
        id SERIAL PRIMARY KEY,
        dandi_id VARCHAR(255) NOT NULL,
        dandi_title TEXT,
        paper_doi TEXT NOT NULL,
        doi_source TEXT,
        relation_type TEXT,
        resolved_at TIMESTAMPTZ DEFAULT NOW(),
        run_id TEXT,
        UNIQUE (dandi_id, paper_doi),
        FOREIGN KEY (paper_doi) REFERENCES papers(paper_doi) ON DELETE CASCADE
    );

    -- Allow schema evolution
    ALTER TABLE dandi_paper_map ADD COLUMN IF NOT EXISTS doi_source TEXT;

    CREATE INDEX IF NOT EXISTS idx_dandi_paper_map_dandi_id ON dandi_paper_map(dandi_id);
    CREATE INDEX IF NOT EXISTS idx_dandi_paper_map_paper_doi ON dandi_paper_map(paper_doi);
    CREATE INDEX IF NOT EXISTS idx_papers_openalex_id ON papers(openalex_id);

    -- Aggregate view: one row per dandiset with aggregated papers
    CREATE OR REPLACE VIEW dandi_dataset_papers AS
    SELECT
        d.dataset_id AS dandi_id,
        d.title AS dandi_title,
        ARRAY_AGG(m.paper_doi ORDER BY m.paper_doi) FILTER (WHERE m.paper_doi IS NOT NULL) AS paper_dois,
        ARRAY_AGG(p.title ORDER BY m.paper_doi) FILTER (WHERE m.paper_doi IS NOT NULL) AS paper_titles
    FROM dandi_dataset d
    LEFT JOIN dandi_paper_map m ON d.dataset_id = m.dandi_id
    LEFT JOIN papers p ON m.paper_doi = p.paper_doi
    GROUP BY d.dataset_id, d.title;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(ddl)
        conn.commit()
    logger.info("Ensured paper mapping tables/views exist.")


def fetch_candidate_dandi_datasets(**context) -> Dict[str, Any]:
    """
    Load candidate dandisets from Postgres.

    Defaults:
    - only process new datasets not present in dandi_paper_map
    - only iterate up to max_datasets_per_run (default 50) after filtering
    """
    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    max_datasets_per_run = int(params.get("max_datasets_per_run", 50))
    # Hard cap to prevent huge XCom payloads / accidental overload.
    if max_datasets_per_run > 200:
        logger.warning("Capping max_datasets_per_run from %d to 200 for safety.", max_datasets_per_run)
        max_datasets_per_run = 200
    include_already_mapped = bool(params.get("include_already_mapped", False))

    # Over-fetch to allow filtering while still reaching the max.
    prefetch = max(50, max_datasets_per_run * 10)

    base_where = ""
    if not include_already_mapped:
        base_where = """
        WHERE NOT EXISTS (
            SELECT 1 FROM dandi_paper_map m WHERE m.dandi_id = d.dataset_id
        )
        """

    query = f"""
    SELECT
        d.dataset_id,
        d.title,
        d.description,
        d.url,
        d.updated_at,
        d.version
    FROM dandi_dataset d
    {base_where}
    ORDER BY d.updated_at DESC NULLS LAST, d.dataset_id ASC
    LIMIT %s;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (prefetch,))
            rows = cursor.fetchall()

    # Allow runtime override of filter keywords (comma-separated string).
    exclude_kw_raw = params.get("exclude_keywords")
    exclude_keywords = _TEST_DUMMY_KEYWORDS
    if isinstance(exclude_kw_raw, str) and exclude_kw_raw.strip():
        exclude_keywords = tuple([k.strip().lower() for k in exclude_kw_raw.split(",") if k.strip()])

    candidates: List[Dict[str, Any]] = []
    filtered_counts: Dict[str, int] = {}
    for row in rows:
        ds_id, title, description, url, updated_at, version = row
        filtered, reason = keyword_filter_dataset(title, description, exclude_keywords)
        if filtered:
            filtered_counts[reason or "filtered"] = filtered_counts.get(reason or "filtered", 0) + 1
            continue
        candidates.append(
            {
                "dataset_id": ds_id,
                "title": title,
                "description": description,
                "url": url,
                "updated_at": updated_at.isoformat() if updated_at else None,
                "version": version,
            }
        )
        if len(candidates) >= max_datasets_per_run:
            break

    logger.info(
        "Loaded %d raw candidates (prefetch=%d); selected %d after filtering; filtered_counts=%s",
        len(rows),
        prefetch,
        len(candidates),
        filtered_counts,
    )
    return {
        "raw_count": len(rows),
        "prefetch": prefetch,
        "candidates": candidates,
        "filtered_counts": filtered_counts,
        "filtered_out": int(sum(filtered_counts.values())),
        "exclude_keywords": list(exclude_keywords),
    }


def fetch_unmapped_dandi_ids(**context) -> Dict[str, Any]:
    """
    Fetch DANDI dataset_ids to process (unmapped by default), apply test/dummy filtering,
    and (optionally) apply a cap.

    Returns only a list of dataset_id strings (plus run metadata) to keep XCom light.
    """
    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    include_already_mapped = bool(params.get("include_already_mapped", False))
    max_cap = _parse_max_datasets_per_run(params.get("max_datasets_per_run", 50))
    batch_size = _parse_batch_size(params.get("batch_size", 25), default=25)

    # Allow runtime override of filter keywords (comma-separated string).
    exclude_kw_raw = params.get("exclude_keywords")
    exclude_keywords = _TEST_DUMMY_KEYWORDS
    if isinstance(exclude_kw_raw, str) and exclude_kw_raw.strip():
        exclude_keywords = tuple([k.strip().lower() for k in exclude_kw_raw.split(",") if k.strip()])

    base_where = ""
    if not include_already_mapped:
        base_where = """
        WHERE NOT EXISTS (
            SELECT 1 FROM dandi_paper_map m WHERE m.dandi_id = d.dataset_id
        )
        """

    query = f"""
    SELECT d.dataset_id, d.title, d.description, d.updated_at, d.version
    FROM dandi_dataset d
    {base_where}
    ORDER BY d.updated_at DESC NULLS LAST, d.dataset_id ASC;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    filtered_counts: Dict[str, int] = {}
    dataset_ids: List[str] = []
    for (ds_id, title, description, _updated_at, _version) in rows:
        filtered, reason = keyword_filter_dataset(title, description, exclude_keywords)
        if filtered:
            filtered_counts[reason or "filtered"] = filtered_counts.get(reason or "filtered", 0) + 1
            continue
        dataset_ids.append(str(ds_id))
        if max_cap is not None and len(dataset_ids) >= max_cap:
            break

    raw_count = len(rows)
    filtered_out = int(sum(filtered_counts.values()))

    run_id_raw = (context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else "manual"))
    run_id = _sanitize_run_id(str(run_id_raw))
    output_dir = _get_output_root() / run_id

    # Initialize run record early (so the UI has something to show)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO dandi_paper_resolution_runs (run_id, started_at, max_datasets_per_run, candidates_loaded, filtered_out, output_path, summary)
                VALUES (%s, NOW(), %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET started_at = EXCLUDED.started_at;
                """,
                (
                    run_id,
                    max_cap,
                    raw_count,
                    filtered_out,
                    str(output_dir),
                    json.dumps(
                        {
                            "filtered_counts": filtered_counts,
                            "batch_size": batch_size,
                            "include_already_mapped": include_already_mapped,
                        }
                    ),
                ),
            )
        conn.commit()

    logger.info(
        "Selected %d dataset_ids (raw_loaded=%d filtered_out=%d filtered_counts=%s max_cap=%s batch_size=%d include_already_mapped=%s)",
        len(dataset_ids),
        raw_count,
        filtered_out,
        filtered_counts,
        max_cap if max_cap is not None else "ALL",
        batch_size,
        include_already_mapped,
    )

    return {
        "run_id": run_id,
        "output_dir": str(output_dir),
        "raw_count": raw_count,
        "filtered_out": filtered_out,
        "filtered_counts": filtered_counts,
        "dataset_ids": dataset_ids,
        "selected_count": len(dataset_ids),
        "max_datasets_per_run": max_cap,
        "batch_size": batch_size,
        "exclude_keywords": list(exclude_keywords),
        "include_already_mapped": include_already_mapped,
    }


def build_batches(**context) -> List[Dict[str, Any]]:
    """
    Chunk dataset_ids into batch descriptors suitable for dynamic task mapping.
    Each element becomes op_kwargs for a mapped task.
    """
    ti = context["ti"]
    payload: Dict[str, Any] = ti.xcom_pull(task_ids="fetch_unmapped_dandi_ids") or {}
    dataset_ids: List[str] = payload.get("dataset_ids") or []
    run_id: str = payload.get("run_id") or _sanitize_run_id(
        str(context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else "manual"))
    )

    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    batch_size = _parse_batch_size(params.get("batch_size", payload.get("batch_size", 25)), default=25)

    batches: List[Dict[str, Any]] = []
    for i in range(0, len(dataset_ids), batch_size):
        batch_ids = dataset_ids[i : i + batch_size]
        batches.append({"batch_index": int(i // batch_size), "dataset_ids": batch_ids, "run_id": run_id})

    logger.info("Built %d batches (batch_size=%d total_ids=%d)", len(batches), batch_size, len(dataset_ids))
    return batches


def resolve_papers_for_dandi(**context) -> Dict[str, Any]:
    """
    For each candidate dandiset, resolve associated paper DOIs + paper metadata.

    Returns a small-ish structure via XCom (safe for the default 10 dataset run).
    """
    ti = context["ti"]
    fetched: Dict[str, Any] = ti.xcom_pull(task_ids="fetch_candidate_dandi_datasets") or {}
    candidates: List[Dict[str, Any]] = fetched.get("candidates") or []
    filtered_counts: Dict[str, int] = fetched.get("filtered_counts") or {}
    filtered_out: int = int(fetched.get("filtered_out") or 0)
    raw_count: int = int(fetched.get("raw_count") or len(candidates))

    run_id_raw = (context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else "manual"))
    run_id = _sanitize_run_id(str(run_id_raw))
    # We no longer write mapping artifacts to disk by default (mappings live in Postgres).
    # Keep output_dir for compatibility/logging only.
    output_dir = _get_output_root() / run_id

    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    write_run_artifacts = bool(params.get("write_run_artifacts", False))
    include_secondary_relations = bool(params.get("include_secondary_relations", False))
    min_interval_seconds = float(params.get("min_api_interval_seconds", 0.2))
    max_retries = int(params.get("max_retries", 6))
    backoff_seconds = float(params.get("backoff_seconds", 2.0))
    # Safety caps for reliability knobs
    max_retries = min(max_retries, 12)
    backoff_seconds = min(backoff_seconds, 10.0)

    # Initialize run record early
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO dandi_paper_resolution_runs (run_id, started_at, max_datasets_per_run, candidates_loaded, filtered_out, output_path, summary)
                VALUES (%s, NOW(), %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET started_at = EXCLUDED.started_at;
                """,
                (
                    run_id,
                    int(params.get("max_datasets_per_run", 50)),
                    raw_count,
                    filtered_out,
                    str(output_dir),
                    json.dumps({"filtered_counts": filtered_counts}),
                ),
            )
        conn.commit()

    resolved_mappings: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []

    # Aggregate telemetry across all datasets
    telemetry = {
        "api_429_count": 0,
        "api_5xx_count": 0,
        "api_retry_count": 0,
        "throttled_count": 0,
        "throttled_sleep_seconds": 0.0,
        "total_requests": 0,
    }

    total = len(candidates)
    logger.info(
        "Resolving papers for %d dandisets (run_id=%s output_dir=%s raw_candidates=%d filtered_out=%d filtered_counts=%s)",
        total,
        run_id,
        output_dir,
        raw_count,
        filtered_out,
        filtered_counts,
    )

    for i, ds in enumerate(candidates, start=1):
        ds_id = ds.get("dataset_id")
        ds_title = ds.get("title")
        ds_desc = ds.get("description")
        ds_version = ds.get("version")

        logger.info("Processing dandiset %d/%d: %s title=%r", i, total, ds_id, (ds_title or "")[:120])

        try:
            result: PaperResolutionResult = resolve_papers_for_dandiset(
                dandiset_id=str(ds_id),
                dandiset_title=ds_title,
                dandiset_description=ds_desc,
                dandiset_version=ds_version,
                include_secondary_relations=include_secondary_relations,
                min_interval_seconds=min_interval_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )

            # Update global telemetry
            for k in telemetry.keys():
                telemetry[k] += result.telemetry.get(k, 0)  # type: ignore[operator]

            if not result.papers:
                unresolved.append(
                    {
                        "dandi_id": str(ds_id),
                        "dandi_title": ds_title,
                        "reason": result.reason or "no_papers_found",
                        "error": result.error,
                    }
                )
                logger.info("No papers resolved for %s (reason=%s)", ds_id, result.reason)
            else:
                for p in result.papers:
                    resolved_mappings.append(
                        {
                            "dandi_id": str(ds_id),
                            "dandi_title": ds_title,
                            "paper_doi": p.get("doi"),
                            "paper_title": p.get("title"),
                            "openalex_id": p.get("openalex_id"),
                            "authors": p.get("authors"),
                            # source of DOI extraction (relatedResource vs description)
                            "doi_source": p.get("source"),
                            # source of paper metadata resolution (crossref vs openalex)
                            "paper_metadata_source": p.get("paper_metadata_source"),
                            "relation_type": p.get("relation_type"),
                            "resolved_at": _utc_now_iso(),
                            "run_id": run_id,
                        }
                    )

            # Optional per-dandiset artifact for debugging only (off by default).
            if write_run_artifacts:
                output_dir.mkdir(parents=True, exist_ok=True)
                per_ds_path = output_dir / f"dandiset_{ds_id}.json"
                with open(per_ds_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "dandi_id": str(ds_id),
                            "dandi_title": ds_title,
                            "dandi_version": ds_version,
                            "papers": result.papers,
                            "reason": result.reason,
                            "error": result.error,
                            "telemetry": result.telemetry,
                            "resolved_at": _utc_now_iso(),
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )

        except Exception as e:
            unresolved.append(
                {
                    "dandi_id": str(ds_id),
                    "dandi_title": ds_title,
                    "reason": "exception",
                    "error": str(e),
                }
            )
            logger.exception("Exception resolving papers for dandiset %s", ds_id)

    logger.info(
        "Resolution complete: dandisets=%d resolved_mappings=%d unresolved_dandisets=%d telemetry=%s",
        total,
        len(resolved_mappings),
        len(unresolved),
        telemetry,
    )

    return {
        "run_id": run_id,
        "output_dir": str(output_dir),
        "raw_candidates_loaded": raw_count,
        "filtered_out": filtered_out,
        "filtered_counts": filtered_counts,
        "candidates_processed": total,
        "resolved_mappings": resolved_mappings,
        "unresolved_dandisets": unresolved,
        "telemetry": telemetry,
    }


def persist_paper_mappings(**context) -> Dict[str, Any]:
    """
    Upsert paper records and dandi->paper mappings into Postgres.
    """
    ti = context["ti"]
    payload: Dict[str, Any] = ti.xcom_pull(task_ids="resolve_papers_for_dandi") or {}

    run_id = payload.get("run_id")
    resolved: List[Dict[str, Any]] = payload.get("resolved_mappings") or []
    unresolved: List[Dict[str, Any]] = payload.get("unresolved_dandisets") or []
    telemetry: Dict[str, Any] = payload.get("telemetry") or {}
    output_dir = payload.get("output_dir")

    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    force_refresh_fulltext = bool(params.get("force_refresh_fulltext", False))

    output_root = _get_output_root()

    paper_upsert = """
    INSERT INTO papers (
        paper_doi, openalex_id, title, authors,
        fulltext_cache_key, fulltext_cached_at, fulltext_source, fulltext_available, fulltext_reason,
        source, fetched_at
    )
    VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (paper_doi) DO UPDATE SET
        openalex_id = COALESCE(EXCLUDED.openalex_id, papers.openalex_id),
        title = COALESCE(EXCLUDED.title, papers.title),
        authors = COALESCE(EXCLUDED.authors, papers.authors),
        fulltext_cache_key = COALESCE(EXCLUDED.fulltext_cache_key, papers.fulltext_cache_key),
        fulltext_cached_at = COALESCE(EXCLUDED.fulltext_cached_at, papers.fulltext_cached_at),
        fulltext_source = COALESCE(EXCLUDED.fulltext_source, papers.fulltext_source),
        fulltext_available = COALESCE(EXCLUDED.fulltext_available, papers.fulltext_available),
        fulltext_reason = COALESCE(EXCLUDED.fulltext_reason, papers.fulltext_reason),
        source = COALESCE(EXCLUDED.source, papers.source),
        fetched_at = NOW();
    """

    map_upsert = """
    INSERT INTO dandi_paper_map (dandi_id, dandi_title, paper_doi, doi_source, relation_type, resolved_at, run_id)
    VALUES (%s, %s, %s, %s, %s, NOW(), %s)
    ON CONFLICT (dandi_id, paper_doi) DO UPDATE SET
        dandi_title = COALESCE(EXCLUDED.dandi_title, dandi_paper_map.dandi_title),
        doi_source = COALESCE(EXCLUDED.doi_source, dandi_paper_map.doi_source),
        relation_type = COALESCE(EXCLUDED.relation_type, dandi_paper_map.relation_type),
        resolved_at = NOW(),
        run_id = EXCLUDED.run_id;
    """

    inserted_papers = 0
    inserted_maps = 0

    # Full-text caching metrics (stored into run summary)
    papers_already_cached = 0
    papers_fulltext_fetched = 0
    papers_fulltext_unavailable = 0
    fulltext_source_counts: Dict[str, int] = {}

    # Avoid refetching same DOI multiple times within a run
    processed_dois: set[str] = set()
    # Track dandisets touched by this run so we can update `dandi_dataset.papers`
    processed_dandisets: set[str] = set()

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for rec in resolved:
                doi = rec.get("paper_doi")
                if not doi:
                    continue
                doi_norm = normalize_doi(doi) or doi
                dandi_id = rec.get("dandi_id")
                if isinstance(dandi_id, str) and dandi_id:
                    processed_dandisets.add(dandi_id)

                # Paper-level upsert + caching is done once per DOI
                if doi_norm not in processed_dois:
                    processed_dois.add(doi_norm)

                    # Check cache first (DB-backed)
                    cursor.execute(
                        """
                        SELECT fulltext_cache_key
                        FROM papers
                        WHERE paper_doi = %s
                        """,
                        (doi_norm,),
                    )
                    row = cursor.fetchone()
                    existing_cache_key = row[0] if row else None

                    cache_key = existing_cache_key
                    fulltext_cached_at = None
                    fulltext_source = None
                    fulltext_available = None
                    fulltext_reason = None

                    if existing_cache_key and not force_refresh_fulltext:
                        papers_already_cached += 1
                    else:
                        # Compute deterministic cache key and write cache JSON
                        cache_key = paper_cache_key_for_doi(doi_norm)
                        if not cache_key:
                            fulltext_available = False
                            fulltext_source = "none"
                            fulltext_reason = "invalid_doi"
                            papers_fulltext_unavailable += 1
                        else:
                            # Fetch OA full text (EuropePMC/PMC) and store JSON cache
                            tel = Telemetry()
                            session = requests.Session()
                            full_text, src, available, reason = fetch_fulltext_oa(
                                session,
                                doi_norm,
                                telemetry=tel,
                                min_interval_seconds=float(params.get("min_api_interval_seconds", 0.2)),
                                max_retries=int(params.get("max_retries", 6)),
                                backoff_seconds=float(params.get("backoff_seconds", 2.0)),
                            )

                            fulltext_source = src
                            fulltext_available = bool(available)
                            fulltext_reason = reason

                            if available:
                                papers_fulltext_fetched += 1
                            else:
                                papers_fulltext_unavailable += 1

                            fulltext_source_counts[src] = fulltext_source_counts.get(src, 0) + 1

                            # Write cache JSON to disk under output_root/<cache_key>
                            cache_path = output_root / cache_key
                            cache_path.parent.mkdir(parents=True, exist_ok=True)
                            with open(cache_path, "w", encoding="utf-8") as f:
                                json.dump(
                                    {
                                        "doi": doi_norm,
                                        "title": rec.get("paper_title"),
                                        "authors": rec.get("authors"),
                                        "canonical_url": f"https://doi.org/{doi_norm}",
                                        "openalex_id": rec.get("openalex_id"),
                                        "paper_metadata_source": rec.get("paper_metadata_source"),
                                        "full_text": full_text,
                                        "full_text_source": src,
                                        "full_text_available": bool(available),
                                        "full_text_reason": reason,
                                        "cached_at": _utc_now_iso(),
                                    },
                                    f,
                                    ensure_ascii=False,
                                )

                            # Mark cached timestamp (DB)
                            fulltext_cached_at = datetime.now(timezone.utc)

                    cursor.execute(
                        paper_upsert,
                        (
                            doi_norm,
                            rec.get("openalex_id"),
                            rec.get("paper_title"),
                            json.dumps(rec.get("authors")) if rec.get("authors") is not None else None,
                            cache_key,
                            fulltext_cached_at,
                            fulltext_source,
                            fulltext_available,
                            fulltext_reason,
                            rec.get("paper_metadata_source"),
                        ),
                    )
                    inserted_papers += 1

                cursor.execute(
                    map_upsert,
                    (
                        dandi_id,
                        rec.get("dandi_title"),
                        doi_norm,
                        rec.get("doi_source"),
                        rec.get("relation_type"),
                        run_id,
                    ),
                )
                inserted_maps += 1

            # Also track dandisets that had no resolved mappings (so we can set papers=0)
            for u in unresolved:
                ds_id = u.get("dandi_id")
                if isinstance(ds_id, str) and ds_id:
                    processed_dandisets.add(ds_id)

            # Update dandi_dataset.papers for dandisets touched this run.
            # We set to 0 first, then overwrite with actual counts where present.
            if processed_dandisets:
                ds_ids = sorted(processed_dandisets)
                placeholders = ", ".join(["%s"] * len(ds_ids))

                cursor.execute(
                    f"UPDATE dandi_dataset SET papers = 0 WHERE dataset_id IN ({placeholders});",
                    ds_ids,
                )
                cursor.execute(
                    f"""
                    UPDATE dandi_dataset d
                    SET papers = sub.cnt
                    FROM (
                        SELECT dandi_id, COUNT(*)::int AS cnt
                        FROM dandi_paper_map
                        WHERE dandi_id IN ({placeholders})
                        GROUP BY dandi_id
                    ) sub
                    WHERE d.dataset_id = sub.dandi_id;
                    """,
                    ds_ids,
                )

            # Update run record
            cursor.execute(
                """
                UPDATE dandi_paper_resolution_runs
                SET
                    finished_at = NOW(),
                    candidates_processed = %s,
                    filtered_out = %s,
                    resolved_mappings = %s,
                    unresolved_datasets = %s,
                    api_429_count = %s,
                    api_5xx_count = %s,
                    api_retry_count = %s,
                    api_throttled_sleep_seconds = %s,
                    output_path = %s,
                    summary = %s
                WHERE run_id = %s;
                """,
                (
                    int(payload.get("candidates_processed", 0)),
                    int(payload.get("filtered_out", 0)),
                    len(resolved),
                    len(unresolved),
                    int(telemetry.get("api_429_count", 0)),
                    int(telemetry.get("api_5xx_count", 0)),
                    int(telemetry.get("api_retry_count", 0)),
                    float(telemetry.get("throttled_sleep_seconds", 0.0)),
                    str(output_dir),
                    json.dumps(
                        {
                            "inserted_papers": inserted_papers,
                            "inserted_mappings": inserted_maps,
                            "telemetry": telemetry,
                            "filtered_counts": payload.get("filtered_counts") or {},
                            "papers_already_cached": papers_already_cached,
                            "papers_fulltext_fetched": papers_fulltext_fetched,
                            "papers_fulltext_unavailable": papers_fulltext_unavailable,
                            "fulltext_source_counts": fulltext_source_counts,
                            "paper_cache_root": str(output_root),
                            "dandisets_papers_updated": len(processed_dandisets),
                        }
                    ),
                    run_id,
                ),
            )
        conn.commit()

    logger.info(
        "Persisted mappings: papers_upserted=%d mappings_upserted=%d unresolved_dandisets=%d",
        inserted_papers,
        inserted_maps,
        len(unresolved),
    )

    return {
        "run_id": run_id,
        "output_dir": output_dir,
        "papers_upserted": inserted_papers,
        "mappings_upserted": inserted_maps,
        "unresolved_dandisets": len(unresolved),
    }


def _persist_resolved_records(
    *,
    resolved: List[Dict[str, Any]],
    unresolved: List[Dict[str, Any]],
    run_id: str,
    output_dir: Path,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Persist resolved records (papers + mappings) and update dandi_dataset.papers counts.

    This is the DB-first persistence path used by dynamic task mapping.
    It intentionally does NOT update dandi_paper_resolution_runs (done in summarize task).
    """
    force_refresh_fulltext = bool(params.get("force_refresh_fulltext", False))
    output_root = _get_output_root()

    paper_upsert = """
    INSERT INTO papers (
        paper_doi, openalex_id, title, authors,
        fulltext_cache_key, fulltext_cached_at, fulltext_source, fulltext_available, fulltext_reason,
        source, fetched_at
    )
    VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (paper_doi) DO UPDATE SET
        openalex_id = COALESCE(EXCLUDED.openalex_id, papers.openalex_id),
        title = COALESCE(EXCLUDED.title, papers.title),
        authors = COALESCE(EXCLUDED.authors, papers.authors),
        fulltext_cache_key = COALESCE(EXCLUDED.fulltext_cache_key, papers.fulltext_cache_key),
        fulltext_cached_at = COALESCE(EXCLUDED.fulltext_cached_at, papers.fulltext_cached_at),
        fulltext_source = COALESCE(EXCLUDED.fulltext_source, papers.fulltext_source),
        fulltext_available = COALESCE(EXCLUDED.fulltext_available, papers.fulltext_available),
        fulltext_reason = COALESCE(EXCLUDED.fulltext_reason, papers.fulltext_reason),
        source = COALESCE(EXCLUDED.source, papers.source),
        fetched_at = NOW();
    """

    map_upsert = """
    INSERT INTO dandi_paper_map (dandi_id, dandi_title, paper_doi, doi_source, relation_type, resolved_at, run_id)
    VALUES (%s, %s, %s, %s, %s, NOW(), %s)
    ON CONFLICT (dandi_id, paper_doi) DO UPDATE SET
        dandi_title = COALESCE(EXCLUDED.dandi_title, dandi_paper_map.dandi_title),
        doi_source = COALESCE(EXCLUDED.doi_source, dandi_paper_map.doi_source),
        relation_type = COALESCE(EXCLUDED.relation_type, dandi_paper_map.relation_type),
        resolved_at = NOW(),
        run_id = EXCLUDED.run_id;
    """

    inserted_papers = 0
    inserted_maps = 0

    # Full-text caching metrics (per batch; aggregated later)
    papers_already_cached = 0
    papers_fulltext_fetched = 0
    papers_fulltext_unavailable = 0
    fulltext_source_counts: Dict[str, int] = {}

    processed_dois: set[str] = set()
    processed_dandisets: set[str] = set()

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for rec in resolved:
                doi = rec.get("paper_doi")
                if not doi:
                    continue
                doi_norm = normalize_doi(doi) or doi
                dandi_id = rec.get("dandi_id")
                if isinstance(dandi_id, str) and dandi_id:
                    processed_dandisets.add(dandi_id)

                # Paper-level upsert + caching is done once per DOI per batch
                if doi_norm not in processed_dois:
                    processed_dois.add(doi_norm)

                    cursor.execute(
                        """
                        SELECT fulltext_cache_key
                        FROM papers
                        WHERE paper_doi = %s
                        """,
                        (doi_norm,),
                    )
                    row = cursor.fetchone()
                    existing_cache_key = row[0] if row else None

                    cache_key = existing_cache_key
                    fulltext_cached_at = None
                    fulltext_source = None
                    fulltext_available = None
                    fulltext_reason = None

                    if existing_cache_key and not force_refresh_fulltext:
                        papers_already_cached += 1
                    else:
                        cache_key = paper_cache_key_for_doi(doi_norm)
                        if not cache_key:
                            fulltext_available = False
                            fulltext_source = "none"
                            fulltext_reason = "invalid_doi"
                            papers_fulltext_unavailable += 1
                        else:
                            tel = Telemetry()
                            session = requests.Session()
                            full_text, src, available, reason = fetch_fulltext_oa(
                                session,
                                doi_norm,
                                telemetry=tel,
                                min_interval_seconds=float(params.get("min_api_interval_seconds", 0.2)),
                                max_retries=int(params.get("max_retries", 6)),
                                backoff_seconds=float(params.get("backoff_seconds", 2.0)),
                            )

                            fulltext_source = src
                            fulltext_available = bool(available)
                            fulltext_reason = reason

                            if available:
                                papers_fulltext_fetched += 1
                            else:
                                papers_fulltext_unavailable += 1

                            fulltext_source_counts[src] = fulltext_source_counts.get(src, 0) + 1

                            cache_path = output_root / cache_key
                            cache_path.parent.mkdir(parents=True, exist_ok=True)
                            with open(cache_path, "w", encoding="utf-8") as f:
                                json.dump(
                                    {
                                        "doi": doi_norm,
                                        "title": rec.get("paper_title"),
                                        "authors": rec.get("authors"),
                                        "canonical_url": f"https://doi.org/{doi_norm}",
                                        "openalex_id": rec.get("openalex_id"),
                                        "paper_metadata_source": rec.get("paper_metadata_source"),
                                        "full_text": full_text,
                                        "full_text_source": src,
                                        "full_text_available": bool(available),
                                        "full_text_reason": reason,
                                        "cached_at": _utc_now_iso(),
                                    },
                                    f,
                                    ensure_ascii=False,
                                )

                            fulltext_cached_at = datetime.now(timezone.utc)

                    cursor.execute(
                        paper_upsert,
                        (
                            doi_norm,
                            rec.get("openalex_id"),
                            rec.get("paper_title"),
                            json.dumps(rec.get("authors")) if rec.get("authors") is not None else None,
                            cache_key,
                            fulltext_cached_at,
                            fulltext_source,
                            fulltext_available,
                            fulltext_reason,
                            rec.get("paper_metadata_source"),
                        ),
                    )
                    inserted_papers += 1

                cursor.execute(
                    map_upsert,
                    (
                        dandi_id,
                        rec.get("dandi_title"),
                        doi_norm,
                        rec.get("doi_source"),
                        rec.get("relation_type"),
                        run_id,
                    ),
                )
                inserted_maps += 1

            # Track dandisets that had no resolved mappings so we can set papers=0
            for u in unresolved:
                ds_id = u.get("dandi_id")
                if isinstance(ds_id, str) and ds_id:
                    processed_dandisets.add(ds_id)

            if processed_dandisets:
                ds_ids = sorted(processed_dandisets)
                placeholders = ", ".join(["%s"] * len(ds_ids))
                cursor.execute(
                    f"UPDATE dandi_dataset SET papers = 0 WHERE dataset_id IN ({placeholders});",
                    ds_ids,
                )
                cursor.execute(
                    f"""
                    UPDATE dandi_dataset d
                    SET papers = sub.cnt
                    FROM (
                        SELECT dandi_id, COUNT(*)::int AS cnt
                        FROM dandi_paper_map
                        WHERE dandi_id IN ({placeholders})
                        GROUP BY dandi_id
                    ) sub
                    WHERE d.dataset_id = sub.dandi_id;
                    """,
                    ds_ids,
                )

        conn.commit()

    return {
        "papers_upserted": inserted_papers,
        "mappings_upserted": inserted_maps,
        "unresolved_dandisets": len(unresolved),
        "unique_dois_processed": len(processed_dois),
        "papers_already_cached": papers_already_cached,
        "papers_fulltext_fetched": papers_fulltext_fetched,
        "papers_fulltext_unavailable": papers_fulltext_unavailable,
        "fulltext_source_counts": fulltext_source_counts,
        "paper_cache_root": str(output_root),
        "output_dir": str(output_dir),
    }


def resolve_and_persist_batch(*, batch_index: int, dataset_ids: List[str], run_id: str, **context) -> Dict[str, Any]:
    """
    Mapped task: resolve papers for a batch of dataset_ids and persist results to Postgres.
    Returns small metrics for final aggregation.
    """
    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    include_secondary_relations = bool(params.get("include_secondary_relations", False))
    min_interval_seconds = float(params.get("min_api_interval_seconds", 0.2))
    max_retries = min(int(params.get("max_retries", 6)), 12)
    backoff_seconds = min(float(params.get("backoff_seconds", 2.0)), 10.0)
    write_run_artifacts = bool(params.get("write_run_artifacts", False))

    output_dir = _get_output_root() / run_id

    # Load metadata for the batch in one DB round-trip
    meta_by_id: Dict[str, Dict[str, Any]] = {}
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT dataset_id, title, description, url, updated_at, version
                FROM dandi_dataset
                WHERE dataset_id = ANY(%s);
                """,
                (dataset_ids,),
            )
            for (ds_id, title, description, url, updated_at, version) in cursor.fetchall():
                meta_by_id[str(ds_id)] = {
                    "dataset_id": str(ds_id),
                    "title": title,
                    "description": description,
                    "url": url,
                    "updated_at": updated_at.isoformat() if updated_at else None,
                    "version": version,
                }

    resolved_mappings: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []
    telemetry = {
        "api_429_count": 0,
        "api_5xx_count": 0,
        "api_retry_count": 0,
        "throttled_count": 0,
        "throttled_sleep_seconds": 0.0,
        "total_requests": 0,
    }

    for i, ds_id in enumerate(dataset_ids, start=1):
        meta = meta_by_id.get(str(ds_id))
        if not meta:
            unresolved.append({"dandi_id": str(ds_id), "dandi_title": None, "reason": "missing_in_db", "error": None})
            continue

        ds_title = meta.get("title")
        ds_desc = meta.get("description")
        ds_version = meta.get("version")

        logger.info("Batch %d: processing %d/%d dandiset=%s title=%r", batch_index, i, len(dataset_ids), ds_id, (ds_title or "")[:120])
        try:
            result: PaperResolutionResult = resolve_papers_for_dandiset(
                dandiset_id=str(ds_id),
                dandiset_title=ds_title,
                dandiset_description=ds_desc,
                dandiset_version=ds_version,
                include_secondary_relations=include_secondary_relations,
                min_interval_seconds=min_interval_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )

            for k in telemetry.keys():
                telemetry[k] += result.telemetry.get(k, 0)  # type: ignore[operator]

            if not result.papers:
                unresolved.append(
                    {
                        "dandi_id": str(ds_id),
                        "dandi_title": ds_title,
                        "reason": result.reason or "no_papers_found",
                        "error": result.error,
                    }
                )
            else:
                for p in result.papers:
                    resolved_mappings.append(
                        {
                            "dandi_id": str(ds_id),
                            "dandi_title": ds_title,
                            "paper_doi": p.get("doi"),
                            "paper_title": p.get("title"),
                            "openalex_id": p.get("openalex_id"),
                            "authors": p.get("authors"),
                            "doi_source": p.get("source"),
                            "paper_metadata_source": p.get("paper_metadata_source"),
                            "relation_type": p.get("relation_type"),
                            "resolved_at": _utc_now_iso(),
                            "run_id": run_id,
                        }
                    )

            if write_run_artifacts:
                output_dir.mkdir(parents=True, exist_ok=True)
                per_ds_path = output_dir / f"dandiset_{ds_id}.json"
                with open(per_ds_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "dandi_id": str(ds_id),
                            "dandi_title": ds_title,
                            "dandi_version": ds_version,
                            "papers": result.papers,
                            "reason": result.reason,
                            "error": result.error,
                            "telemetry": result.telemetry,
                            "resolved_at": _utc_now_iso(),
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )

        except Exception as e:
            unresolved.append({"dandi_id": str(ds_id), "dandi_title": ds_title, "reason": "exception", "error": str(e)})
            logger.exception("Batch %d: exception resolving papers for dandiset %s", batch_index, ds_id)

    persist_metrics = _persist_resolved_records(
        resolved=resolved_mappings,
        unresolved=unresolved,
        run_id=run_id,
        output_dir=output_dir,
        params=params,
    )

    return {
        "batch_index": batch_index,
        "datasets_processed": len(dataset_ids),
        "resolved_mappings": len(resolved_mappings),
        "unresolved_dandisets": len(unresolved),
        "telemetry": telemetry,
        **persist_metrics,
    }


def export_run_artifacts(**context) -> None:
    """
    Optional per-run artifacts.

    By default we do NOT write mapping artifacts to disk (mappings live in Postgres).
    This task is kept as a no-op unless `write_run_artifacts=true`.
    """
    ti = context["ti"]
    payload: Dict[str, Any] = ti.xcom_pull(task_ids="resolve_papers_for_dandi") or {}
    persist: Dict[str, Any] = ti.xcom_pull(task_ids="persist_paper_mappings") or {}

    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    write_run_artifacts = bool(params.get("write_run_artifacts", False))
    if not write_run_artifacts:
        logger.info("Skipping run artifact export (write_run_artifacts=false).")
        return

    run_id = payload.get("run_id")
    output_dir = Path(payload.get("output_dir"))
    resolved: List[Dict[str, Any]] = payload.get("resolved_mappings") or []
    unresolved: List[Dict[str, Any]] = payload.get("unresolved_dandisets") or []
    telemetry: Dict[str, Any] = payload.get("telemetry") or {}

    output_dir.mkdir(parents=True, exist_ok=True)

    summary_path = output_dir / "summary.json"

    summary = {
        "run_id": run_id,
        "output_dir": str(output_dir),
        "raw_candidates_loaded": int(payload.get("raw_candidates_loaded", 0)),
        "filtered_out": int(payload.get("filtered_out", 0)),
        "filtered_counts": payload.get("filtered_counts") or {},
        "candidates_processed": int(payload.get("candidates_processed", 0)),
        "resolved_mappings": len(resolved),
        "unresolved_dandisets": len(unresolved),
        "telemetry": telemetry,
        "persist": persist,
        "paper_cache_root": str(_get_output_root()),
        "generated_at": _utc_now_iso(),
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(
        "Wrote run summary artifact: %s",
        summary_path,
    )


def summarize_run(**context) -> None:
    """
    Aggregate mapped batch metrics, update the run record, and log a final summary.
    """
    ti = context["ti"]
    seed: Dict[str, Any] = ti.xcom_pull(task_ids="fetch_unmapped_dandi_ids") or {}
    run_id = seed.get("run_id") or _sanitize_run_id(
        str(context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else "manual"))
    )
    output_dir = seed.get("output_dir")

    batch_results = ti.xcom_pull(task_ids="resolve_and_persist_batch") or []
    if isinstance(batch_results, dict):
        batch_results = [batch_results]
    if not isinstance(batch_results, list):
        batch_results = []

    totals = {
        "batches": 0,
        "datasets_processed": 0,
        "resolved_mappings": 0,
        "unresolved_dandisets": 0,
        "papers_upserted": 0,
        "mappings_upserted": 0,
        "unique_dois_processed": 0,
        "papers_already_cached": 0,
        "papers_fulltext_fetched": 0,
        "papers_fulltext_unavailable": 0,
        "fulltext_source_counts": {},
    }
    telemetry_totals = {
        "api_429_count": 0,
        "api_5xx_count": 0,
        "api_retry_count": 0,
        "throttled_count": 0,
        "throttled_sleep_seconds": 0.0,
        "total_requests": 0,
    }

    for r in batch_results:
        if not isinstance(r, dict):
            continue
        totals["batches"] += 1
        totals["datasets_processed"] += int(r.get("datasets_processed", 0) or 0)
        totals["resolved_mappings"] += int(r.get("resolved_mappings", 0) or 0)
        totals["unresolved_dandisets"] += int(r.get("unresolved_dandisets", 0) or 0)
        totals["papers_upserted"] += int(r.get("papers_upserted", 0) or 0)
        totals["mappings_upserted"] += int(r.get("mappings_upserted", 0) or 0)
        totals["unique_dois_processed"] += int(r.get("unique_dois_processed", 0) or 0)
        totals["papers_already_cached"] += int(r.get("papers_already_cached", 0) or 0)
        totals["papers_fulltext_fetched"] += int(r.get("papers_fulltext_fetched", 0) or 0)
        totals["papers_fulltext_unavailable"] += int(r.get("papers_fulltext_unavailable", 0) or 0)

        ft = r.get("fulltext_source_counts") or {}
        if isinstance(ft, dict):
            for k, v in ft.items():
                try:
                    totals["fulltext_source_counts"][k] = totals["fulltext_source_counts"].get(k, 0) + int(v)
                except Exception:
                    continue

        tel = r.get("telemetry") or {}
        if isinstance(tel, dict):
            for k in telemetry_totals.keys():
                telemetry_totals[k] += tel.get(k, 0)  # type: ignore[operator]

    # Update run record (single writer at end-of-run)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE dandi_paper_resolution_runs
                SET
                    finished_at = NOW(),
                    candidates_processed = %s,
                    resolved_mappings = %s,
                    unresolved_datasets = %s,
                    api_429_count = %s,
                    api_5xx_count = %s,
                    api_retry_count = %s,
                    api_throttled_sleep_seconds = %s,
                    output_path = %s,
                    summary = %s
                WHERE run_id = %s;
                """,
                (
                    int(totals["datasets_processed"]),
                    int(totals["resolved_mappings"]),
                    int(totals["unresolved_dandisets"]),
                    int(telemetry_totals.get("api_429_count", 0)),
                    int(telemetry_totals.get("api_5xx_count", 0)),
                    int(telemetry_totals.get("api_retry_count", 0)),
                    float(telemetry_totals.get("throttled_sleep_seconds", 0.0)),
                    str(output_dir),
                    json.dumps(
                        {
                            "seed": {
                                "raw_count": seed.get("raw_count"),
                                "filtered_out": seed.get("filtered_out"),
                                "filtered_counts": seed.get("filtered_counts"),
                                "selected_count": seed.get("selected_count"),
                                "max_datasets_per_run": seed.get("max_datasets_per_run"),
                                "batch_size": seed.get("batch_size"),
                                "include_already_mapped": seed.get("include_already_mapped"),
                            },
                            "totals": totals,
                            "telemetry": telemetry_totals,
                            "paper_cache_root": str(_get_output_root()),
                        }
                    ),
                    run_id,
                ),
            )
        conn.commit()

    # Log final human-readable summary.
    logger.info(
        "\n".join(
            [
                "DANDI paper mapping run summary (dynamic mapping)",
                f"- run_id: {run_id}",
                f"- batches: {totals['batches']}",
                f"- datasets reviewed: {totals['datasets_processed']} (raw_loaded={seed.get('raw_count')}, filtered_out={seed.get('filtered_out')})",
                f"- mappings persisted: {totals['mappings_upserted']} (resolved_records={totals['resolved_mappings']}, unresolved_datasets={totals['unresolved_dandisets']})",
                f"- unique papers processed: {totals['unique_dois_processed']}",
                f"- fulltext: fetched={totals['papers_fulltext_fetched']} unavailable={totals['papers_fulltext_unavailable']} already_cached={totals['papers_already_cached']}",
                f"- fulltext sources: {totals['fulltext_source_counts']}",
                f"- telemetry: {telemetry_totals}",
                f"- cache root: {_get_output_root()}",
                f"- run output dir: {output_dir}",
            ]
        )
    )


def verify_paper_mapping(**context) -> None:
    """
    End-of-run summary: log counts and a small sample.
    """
    ti = context["ti"]
    resolve_payload: Dict[str, Any] = ti.xcom_pull(task_ids="resolve_papers_for_dandi") or {}
    persist_payload: Dict[str, Any] = ti.xcom_pull(task_ids="persist_paper_mappings") or {}

    run_id = (
        persist_payload.get("run_id")
        or resolve_payload.get("run_id")
        or _sanitize_run_id(
            str(context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else "manual"))
        )
    )

    datasets_reviewed = int(resolve_payload.get("candidates_processed", 0) or 0)
    raw_candidates_loaded = int(resolve_payload.get("raw_candidates_loaded", 0) or 0)
    filtered_out = int(resolve_payload.get("filtered_out", 0) or 0)
    unresolved_dandisets = len(resolve_payload.get("unresolved_dandisets") or [])

    output_root = _get_output_root()
    output_dir = resolve_payload.get("output_dir")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM dandi_paper_map;")
            map_count_total = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM papers;")
            paper_count_total = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM dandi_paper_map WHERE run_id = %s;", (run_id,))
            map_count_run = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT paper_doi) FROM dandi_paper_map WHERE run_id = %s;", (run_id,))
            unique_papers_run = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT
                    p.fulltext_available,
                    p.fulltext_source,
                    COUNT(*) AS n
                FROM papers p
                WHERE p.paper_doi IN (
                    SELECT DISTINCT paper_doi FROM dandi_paper_map WHERE run_id = %s
                )
                GROUP BY p.fulltext_available, p.fulltext_source
                ORDER BY n DESC;
                """,
                (run_id,),
            )
            fulltext_breakdown = cursor.fetchall()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM papers p
                WHERE p.paper_doi IN (
                    SELECT DISTINCT paper_doi FROM dandi_paper_map WHERE run_id = %s
                )
                AND p.fulltext_cache_key IS NOT NULL;
                """,
                (run_id,),
            )
            cached_keys_run = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT dandi_id, COUNT(*) AS n
                FROM dandi_paper_map
                WHERE run_id = %s
                GROUP BY dandi_id
                ORDER BY n DESC
                LIMIT 5;
                """,
                (run_id,),
            )
            top = cursor.fetchall()

            cursor.execute(
                """
                SELECT p.paper_doi, p.title, p.fulltext_cache_key, p.fulltext_source, p.fulltext_reason
                FROM papers p
                WHERE p.paper_doi IN (
                    SELECT DISTINCT paper_doi FROM dandi_paper_map WHERE run_id = %s
                )
                ORDER BY p.fulltext_available DESC NULLS LAST, p.fulltext_cached_at DESC NULLS LAST
                LIMIT 5;
                """,
                (run_id,),
            )
            sample_papers = cursor.fetchall()

    telemetry = resolve_payload.get("telemetry") or {}

    # Format a compact "what happened" summary for the Airflow logs.
    logger.info(
        "\n".join(
            [
                "DANDI paper mapping run summary",
                f"- run_id: {run_id}",
                f"- datasets reviewed: {datasets_reviewed} (raw_loaded={raw_candidates_loaded}, filtered_out={filtered_out}, unresolved={unresolved_dandisets})",
                f"- mappings persisted (this run): {map_count_run} (total={map_count_total})",
                f"- unique papers processed (this run): {unique_papers_run} (papers table total={paper_count_total})",
                f"- papers with cache keys (this run): {cached_keys_run}",
                f"- fulltext breakdown (available, source, n): {fulltext_breakdown}",
                f"- telemetry: {telemetry}",
                f"- cache root: {output_root}",
                f"- run output dir: {output_dir}",
                f"- top datasets (this run): {top}",
                f"- sample papers (doi, title, cache_key, source, reason): {sample_papers}",
            ]
        )
    )


dag = DAG(
    "dandi_paper_mapping",
    default_args=default_args,
    description="Resolve DANDI dataset -> paper DOI/title mappings",
    schedule="@daily",
    catchup=False,
    tags=["datasets", "dandi", "papers", "mapping"],
    is_paused_upon_creation=False,
    params={
        # Dev-safe default. For production scale, set to "all" (or 0/None) via DAG run config / Variable / env.
        "max_datasets_per_run": 50,
        # Batch granularity for dynamic task mapping (limits task count + resource use)
        "batch_size": 25,
        # If true, process datasets even if they already have mappings
        "include_already_mapped": False,
        # If true, include broader DataCite relation types (IsCitedBy, etc.)
        "include_secondary_relations": False,
        # API pacing / reliability knobs (logged to summary)
        "min_api_interval_seconds": 0.2,
        "max_retries": 6,
        "backoff_seconds": 2.0,
        # If true, refetch OA full text even if already cached
        "force_refresh_fulltext": False,
        # If true, write optional per-run/per-dandiset debug artifacts to disk.
        # Mappings always persist to Postgres; this is only for debugging.
        "write_run_artifacts": False,
    },
)


create_tables_task = PythonOperator(
    task_id="create_paper_mapping_tables",
    python_callable=create_paper_mapping_tables,
    dag=dag,
)

fetch_candidates_task = PythonOperator(
    task_id="fetch_unmapped_dandi_ids",
    python_callable=fetch_unmapped_dandi_ids,
    dag=dag,
)

build_batches_task = PythonOperator(
    task_id="build_batches",
    python_callable=build_batches,
    dag=dag,
)

# Mapped batch task (bounded concurrency via Airflow pool).
if XComArg is None:  # pragma: no cover
    raise RuntimeError(
        "Dynamic task mapping requires XComArg (airflow.models.xcom_arg). "
        "Upgrade Airflow or update imports to enable mapped batch execution."
    )
resolve_and_persist_batch_task = (
    PythonOperator.partial(
        task_id="resolve_and_persist_batch",
        python_callable=resolve_and_persist_batch,
        pool="dandi_paper_api_pool",
        dag=dag,
    ).expand(op_kwargs=XComArg(build_batches_task))
)

summarize_task = PythonOperator(
    task_id="summarize_run",
    python_callable=summarize_run,
    dag=dag,
)


create_tables_task >> fetch_candidates_task >> build_batches_task >> resolve_and_persist_batch_task >> summarize_task

