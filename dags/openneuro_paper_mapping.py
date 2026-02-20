"""
Airflow DAG: Resolve OpenNeuro dataset -> paper DOI/title mappings.

Mirrors `dandi_paper_mapping` but uses `openneuro_dataset` as input.

Production-scale notes:
- To process ALL *unmapped* OpenNeuro datasets, set `max_datasets_per_run` to "all" (or 0/None) in DAG params.
- This DAG uses Airflow dynamic task mapping at *batch* granularity.
- To limit parallel API pressure, this DAG shares the `dandi_paper_api_pool` with `dandi_paper_mapping`.
  Configure that pool with a small slot count (e.g., 1–3) to throttle both DAGs together.
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
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:  # pragma: no cover
    from airflow.operators.python import PythonOperator  # type: ignore

try:
    from airflow.models.xcom_arg import XComArg
except Exception:  # pragma: no cover
    XComArg = None  # type: ignore

from utils.database import get_db_connection
from utils.cache_keys import paper_cache_key_for_doi
from utils.find_reuse_core import normalize_doi, Telemetry
from utils.paper_fulltext import fetch_fulltext_oa
from utils.openneuro_paper_resolution import resolve_papers_for_openneuro_dataset, OpenNeuroPaperResolutionResult

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
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("_")


def _get_output_root() -> Path:
    env = os.getenv("OPENNEURO_PAPER_MAPPING_OUTPUT_DIR", "").strip()
    if env:
        return Path(env)
    return Path(__file__).parent / "output" / "openneuro_paper_mapping"


def _parse_max_datasets_per_run(value: Any) -> Optional[int]:
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
    n = int(value)
    return None if n <= 0 else n


def _parse_batch_size(value: Any, default: int = 25) -> int:
    if value is None:
        return default
    if isinstance(value, str) and value.strip():
        value = int(value.strip())
    n = int(value)
    if n <= 0:
        raise ValueError(f"Invalid batch_size: {value!r}")
    return min(n, 200)


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


def keyword_filter_dataset(
    title: Optional[str], description: Optional[str], keywords: Tuple[str, ...]
) -> Tuple[bool, Optional[str]]:
    hay = " ".join([title or "", description or ""]).strip().lower()
    if not hay:
        return False, None
    for kw in keywords:
        if re.search(rf"\\b{re.escape(kw)}\\b", hay):
            return True, f"keyword:{kw}"
    return False, None


def create_openneuro_paper_mapping_tables(**_context) -> None:
    ddl = """
    -- Ensure OpenNeuro table supports papers count
    ALTER TABLE openneuro_dataset ADD COLUMN IF NOT EXISTS papers INTEGER;
    CREATE INDEX IF NOT EXISTS idx_openneuro_papers ON openneuro_dataset (papers DESC);

    -- Shared papers table (may already exist from DANDI mapping)
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

    CREATE TABLE IF NOT EXISTS openneuro_paper_resolution_runs (
        run_id TEXT PRIMARY KEY,
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        max_datasets_per_run INTEGER,
        candidates_loaded INTEGER,
        filtered_out INTEGER,
        candidates_processed INTEGER,
        resolved_mappings INTEGER,
        unresolved_datasets INTEGER,
        api_429_count INTEGER,
        api_5xx_count INTEGER,
        api_retry_count INTEGER,
        api_throttled_sleep_seconds DOUBLE PRECISION,
        output_path TEXT,
        summary JSONB
    );

    CREATE TABLE IF NOT EXISTS openneuro_paper_map (
        id SERIAL PRIMARY KEY,
        openneuro_id VARCHAR(255) NOT NULL,
        openneuro_title TEXT,
        paper_doi TEXT NOT NULL,
        doi_source TEXT,
        relation_type TEXT,
        resolved_at TIMESTAMPTZ DEFAULT NOW(),
        run_id TEXT,
        UNIQUE (openneuro_id, paper_doi),
        FOREIGN KEY (paper_doi) REFERENCES papers(paper_doi) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_openneuro_paper_map_openneuro_id ON openneuro_paper_map(openneuro_id);
    CREATE INDEX IF NOT EXISTS idx_openneuro_paper_map_paper_doi ON openneuro_paper_map(paper_doi);

    -- Aggregate view: aligned arrays via FILTER
    CREATE OR REPLACE VIEW openneuro_dataset_papers AS
    SELECT
        d.dataset_id AS openneuro_id,
        d.title AS openneuro_title,
        ARRAY_AGG(m.paper_doi ORDER BY m.paper_doi) FILTER (WHERE m.paper_doi IS NOT NULL) AS paper_dois,
        ARRAY_AGG(p.title ORDER BY m.paper_doi) FILTER (WHERE m.paper_doi IS NOT NULL) AS paper_titles
    FROM openneuro_dataset d
    LEFT JOIN openneuro_paper_map m ON d.dataset_id = m.openneuro_id
    LEFT JOIN papers p ON m.paper_doi = p.paper_doi
    GROUP BY d.dataset_id, d.title;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(ddl)
        conn.commit()
    logger.info("Ensured OpenNeuro paper mapping tables/views exist.")


def fetch_unmapped_openneuro_ids(**context) -> Dict[str, Any]:
    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    include_already_mapped = bool(params.get("include_already_mapped", False))
    backfill_missing_paper_titles = bool(params.get("backfill_missing_paper_titles", True))
    max_cap = _parse_max_datasets_per_run(params.get("max_datasets_per_run", 50))
    batch_size = _parse_batch_size(params.get("batch_size", 25), default=25)
    prioritize_doi_signals = bool(params.get("prioritize_doi_signals", True))

    exclude_kw_raw = params.get("exclude_keywords")
    exclude_keywords = _TEST_DUMMY_KEYWORDS
    if isinstance(exclude_kw_raw, str) and exclude_kw_raw.strip():
        exclude_keywords = tuple([k.strip().lower() for k in exclude_kw_raw.split(",") if k.strip()])

    base_where = ""
    if not include_already_mapped:
        base_where = """
        WHERE NOT EXISTS (
            SELECT 1 FROM openneuro_paper_map m WHERE m.openneuro_id = d.dataset_id
        )
        """
    elif backfill_missing_paper_titles:
        # Backfill mode: focus only on datasets whose existing mappings have missing paper titles
        # or non-canonical preprint DOI variants (e.g. `10.1101/...v1`, `...v2.abstract`).
        base_where = """
        WHERE EXISTS (
            SELECT 1
            FROM openneuro_paper_map m
            LEFT JOIN papers p ON m.paper_doi = p.paper_doi
            WHERE m.openneuro_id = d.dataset_id
              AND (
                p.title IS NULL
                OR m.paper_doi ~* '^10\\.1101/.+v\\d+(\\.(abstract|full|pdf))?$'
                OR m.paper_doi ~* '^10\\.1101/.+\\.(abstract|full|pdf)$'
              )
        )
        """

    join_mapped_counts = """
    LEFT JOIN (
        SELECT openneuro_id, COUNT(*)::int AS mapped_papers
        FROM openneuro_paper_map
        GROUP BY openneuro_id
    ) mp ON mp.openneuro_id = d.dataset_id
    """

    order_parts: List[str] = []
    if include_already_mapped:
        # Backfill mode: prioritize datasets that already have mappings so we can
        # normalize DOIs and fill missing titles without scanning lots of unmapped rows.
        order_parts.append("CASE WHEN mp.mapped_papers IS NOT NULL THEN 0 ELSE 1 END")
        order_parts.append("mp.mapped_papers DESC NULLS LAST")
    if prioritize_doi_signals:
        # Dev-friendly: prioritize datasets whose description likely contains a DOI/citation.
        order_parts.append("CASE WHEN d.description ILIKE '%doi%' OR d.description LIKE '%10.%' THEN 0 ELSE 1 END")
    order_parts.append("d.updated_at DESC NULLS LAST")
    order_parts.append("d.dataset_id ASC")
    order_by = ",\n    ".join(order_parts)

    query = f"""
    SELECT d.dataset_id, d.title, d.description, d.updated_at
    FROM openneuro_dataset d
    {join_mapped_counts}
    {base_where}
    ORDER BY {order_by};
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    filtered_counts: Dict[str, int] = {}
    dataset_ids: List[str] = []
    for (ds_id, title, description, _updated_at) in rows:
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

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO openneuro_paper_resolution_runs (run_id, started_at, max_datasets_per_run, candidates_loaded, filtered_out, output_path, summary)
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
        "Selected %d OpenNeuro dataset_ids (raw_loaded=%d filtered_out=%d max_cap=%s batch_size=%d include_already_mapped=%s)",
        len(dataset_ids),
        raw_count,
        filtered_out,
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
        "prioritize_doi_signals": prioritize_doi_signals,
        "backfill_missing_paper_titles": backfill_missing_paper_titles,
    }


def build_batches(**context) -> List[Dict[str, Any]]:
    ti = context["ti"]
    payload: Dict[str, Any] = ti.xcom_pull(task_ids="fetch_unmapped_openneuro_ids") or {}
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


def _persist_openneuro_records(
    *,
    resolved: List[Dict[str, Any]],
    unresolved: List[Dict[str, Any]],
    run_id: str,
    output_dir: Path,
    params: Dict[str, Any],
) -> Dict[str, Any]:
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
    INSERT INTO openneuro_paper_map (openneuro_id, openneuro_title, paper_doi, doi_source, relation_type, resolved_at, run_id)
    VALUES (%s, %s, %s, %s, %s, NOW(), %s)
    ON CONFLICT (openneuro_id, paper_doi) DO UPDATE SET
        openneuro_title = COALESCE(EXCLUDED.openneuro_title, openneuro_paper_map.openneuro_title),
        doi_source = COALESCE(EXCLUDED.doi_source, openneuro_paper_map.doi_source),
        relation_type = COALESCE(EXCLUDED.relation_type, openneuro_paper_map.relation_type),
        resolved_at = NOW(),
        run_id = EXCLUDED.run_id;
    """

    inserted_papers = 0
    inserted_maps = 0

    papers_already_cached = 0
    papers_fulltext_fetched = 0
    papers_fulltext_unavailable = 0
    fulltext_source_counts: Dict[str, int] = {}

    processed_dois: set[str] = set()
    processed_datasets: set[str] = set()

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for rec in resolved:
                doi = rec.get("paper_doi")
                if not doi:
                    continue
                doi_norm = normalize_doi(doi)
                if not doi_norm:
                    continue
                ds_id = rec.get("openneuro_id")
                if isinstance(ds_id, str) and ds_id:
                    processed_datasets.add(ds_id)

                if doi_norm not in processed_dois:
                    processed_dois.add(doi_norm)
                    cursor.execute(
                        "SELECT fulltext_cache_key FROM papers WHERE paper_doi = %s",
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
                        ds_id,
                        rec.get("openneuro_title"),
                        doi_norm,
                        rec.get("doi_source"),
                        rec.get("relation_type"),
                        run_id,
                    ),
                )
                inserted_maps += 1

                # Cleanup non-canonical OpenNeuro preprint DOI variants already in the map table.
                # We canonicalize to `doi_norm` at ingestion time; remove any old `...vN` / `.abstract` variants
                # so the API/UI does not surface broken doi.org links.
                if doi_norm.lower().startswith("10.1101/") and isinstance(ds_id, str) and ds_id:
                    cursor.execute(
                        """
                        DELETE FROM openneuro_paper_map
                        WHERE openneuro_id = %s
                          AND paper_doi <> %s
                          AND (
                            paper_doi ILIKE %s
                            OR paper_doi ILIKE %s
                          );
                        """,
                        (
                            ds_id,
                            doi_norm,
                            f"{doi_norm}v%",
                            f"{doi_norm}.%",
                        ),
                    )

            for u in unresolved:
                ds_id = u.get("openneuro_id")
                if isinstance(ds_id, str) and ds_id:
                    processed_datasets.add(ds_id)

            if processed_datasets:
                ds_ids = sorted(processed_datasets)
                placeholders = ", ".join(["%s"] * len(ds_ids))
                cursor.execute(f"UPDATE openneuro_dataset SET papers = 0 WHERE dataset_id IN ({placeholders});", ds_ids)
                cursor.execute(
                    f"""
                    UPDATE openneuro_dataset d
                    SET papers = sub.cnt
                    FROM (
                        SELECT openneuro_id, COUNT(*)::int AS cnt
                        FROM openneuro_paper_map
                        WHERE openneuro_id IN ({placeholders})
                        GROUP BY openneuro_id
                    ) sub
                    WHERE d.dataset_id = sub.openneuro_id;
                    """,
                    ds_ids,
                )
        conn.commit()

    return {
        "papers_upserted": inserted_papers,
        "mappings_upserted": inserted_maps,
        "unresolved_datasets": len(unresolved),
        "unique_dois_processed": len(processed_dois),
        "papers_already_cached": papers_already_cached,
        "papers_fulltext_fetched": papers_fulltext_fetched,
        "papers_fulltext_unavailable": papers_fulltext_unavailable,
        "fulltext_source_counts": fulltext_source_counts,
        "paper_cache_root": str(output_root),
        "output_dir": str(output_dir),
    }


def resolve_and_persist_batch(*, batch_index: int, dataset_ids: List[str], run_id: str, **context) -> Dict[str, Any]:
    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    min_interval_seconds = float(params.get("min_api_interval_seconds", 0.2))
    max_retries = min(int(params.get("max_retries", 6)), 12)
    backoff_seconds = min(float(params.get("backoff_seconds", 2.0)), 10.0)

    output_dir = _get_output_root() / run_id

    meta_by_id: Dict[str, Dict[str, Any]] = {}
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT dataset_id, title, description, updated_at
                FROM openneuro_dataset
                WHERE dataset_id = ANY(%s);
                """,
                (dataset_ids,),
            )
            for (ds_id, title, description, updated_at) in cursor.fetchall():
                meta_by_id[str(ds_id)] = {
                    "dataset_id": str(ds_id),
                    "title": title,
                    "description": description,
                    "updated_at": updated_at.isoformat() if updated_at else None,
                }

    resolved: List[Dict[str, Any]] = []
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
            unresolved.append({"openneuro_id": str(ds_id), "openneuro_title": None, "reason": "missing_in_db", "error": None})
            continue

        title = meta.get("title")
        desc = meta.get("description")
        logger.info("Batch %d: processing %d/%d openneuro=%s title=%r", batch_index, i, len(dataset_ids), ds_id, (title or "")[:120])
        try:
            result: OpenNeuroPaperResolutionResult = resolve_papers_for_openneuro_dataset(
                dataset_id=str(ds_id),
                dataset_title=title,
                dataset_description=desc,
                min_interval_seconds=min_interval_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            for k in telemetry.keys():
                telemetry[k] += result.telemetry.get(k, 0)  # type: ignore[operator]

            if not result.papers:
                unresolved.append({"openneuro_id": str(ds_id), "openneuro_title": title, "reason": result.reason or "no_papers_found", "error": result.error})
            else:
                for p in result.papers:
                    resolved.append(
                        {
                            "openneuro_id": str(ds_id),
                            "openneuro_title": title,
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
        except Exception as e:
            unresolved.append({"openneuro_id": str(ds_id), "openneuro_title": title, "reason": "exception", "error": str(e)})
            logger.exception("Batch %d: exception resolving papers for openneuro %s", batch_index, ds_id)

    persist_metrics = _persist_openneuro_records(
        resolved=resolved,
        unresolved=unresolved,
        run_id=run_id,
        output_dir=output_dir,
        params=params,
    )

    return {
        "batch_index": batch_index,
        "datasets_processed": len(dataset_ids),
        "resolved_mappings": len(resolved),
        "unresolved_datasets": len(unresolved),
        "telemetry": telemetry,
        **persist_metrics,
    }


def summarize_run(**context) -> None:
    ti = context["ti"]
    seed: Dict[str, Any] = ti.xcom_pull(task_ids="fetch_unmapped_openneuro_ids") or {}
    run_id = seed.get("run_id") or _sanitize_run_id(
        str(context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else "manual"))
    )
    output_dir = seed.get("output_dir")
    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    write_run_artifacts = bool(params.get("write_run_artifacts", False))

    batch_results = ti.xcom_pull(task_ids="resolve_and_persist_batch") or []
    if isinstance(batch_results, dict):
        batch_results = [batch_results]
    if not isinstance(batch_results, list):
        batch_results = []

    totals = {
        "batches": 0,
        "datasets_processed": 0,
        "resolved_mappings": 0,
        "unresolved_datasets": 0,
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
        totals["unresolved_datasets"] += int(r.get("unresolved_datasets", 0) or 0)
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

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE openneuro_paper_resolution_runs
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
                    int(totals["unresolved_datasets"]),
                    int(telemetry_totals.get("api_429_count", 0)),
                    int(telemetry_totals.get("api_5xx_count", 0)),
                    int(telemetry_totals.get("api_retry_count", 0)),
                    float(telemetry_totals.get("throttled_sleep_seconds", 0.0)),
                    str(output_dir),
                    json.dumps({"seed": seed, "totals": totals, "telemetry": telemetry_totals}),
                    run_id,
                ),
            )
        conn.commit()

    if write_run_artifacts:
        try:
            out_dir = Path(str(output_dir)) if output_dir else (_get_output_root() / run_id)
            out_dir.mkdir(parents=True, exist_ok=True)
            summary_path = out_dir / "summary.json"
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"seed": seed, "totals": totals, "telemetry": telemetry_totals},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            logger.info("Wrote run artifacts: %s", str(summary_path))
        except Exception:
            logger.debug("Failed to write run artifacts (write_run_artifacts=true).", exc_info=True)

    logger.info(
        "\\n".join(
            [
                "OpenNeuro paper mapping run summary (dynamic mapping)",
                f"- run_id: {run_id}",
                f"- batches: {totals['batches']}",
                f"- datasets reviewed: {totals['datasets_processed']} (raw_loaded={seed.get('raw_count')}, filtered_out={seed.get('filtered_out')})",
                f"- mappings persisted: {totals['mappings_upserted']} (resolved_records={totals['resolved_mappings']}, unresolved={totals['unresolved_datasets']})",
                f"- unique papers processed: {totals['unique_dois_processed']}",
                f"- fulltext: fetched={totals['papers_fulltext_fetched']} unavailable={totals['papers_fulltext_unavailable']} already_cached={totals['papers_already_cached']}",
                f"- fulltext sources: {totals['fulltext_source_counts']}",
                f"- telemetry: {telemetry_totals}",
                f"- cache root: {_get_output_root()}",
                f"- run output dir: {output_dir}",
            ]
        )
    )


dag = DAG(
    "openneuro_paper_mapping",
    default_args=default_args,
    description="Resolve OpenNeuro dataset -> paper DOI/title mappings",
    schedule="@daily",
    catchup=False,
    tags=["datasets", "openneuro", "papers", "mapping"],
    is_paused_upon_creation=False,
    params={
        "max_datasets_per_run": 50,
        "batch_size": 25,
        "include_already_mapped": False,
        "prioritize_doi_signals": True,
        "backfill_missing_paper_titles": True,
        "min_api_interval_seconds": 0.2,
        "max_retries": 6,
        "backoff_seconds": 2.0,
        "force_refresh_fulltext": False,
        "write_run_artifacts": False,
    },
)

create_tables_task = PythonOperator(
    task_id="create_openneuro_paper_mapping_tables",
    python_callable=create_openneuro_paper_mapping_tables,
    dag=dag,
)

fetch_ids_task = PythonOperator(
    task_id="fetch_unmapped_openneuro_ids",
    python_callable=fetch_unmapped_openneuro_ids,
    dag=dag,
)

build_batches_task = PythonOperator(
    task_id="build_batches",
    python_callable=build_batches,
    dag=dag,
)

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

create_tables_task >> fetch_ids_task >> build_batches_task >> resolve_and_persist_batch_task >> summarize_task

