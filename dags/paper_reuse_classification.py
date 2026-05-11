"""
Paper reuse classification DAG.

Classifies citation edges (and optionally primary-paper relationships) as
PRIMARY / SECONDARY / NEITHER / UNKNOWN using an LLM via the OpenRouter API.

Results are written to the existing ``dandi_paper_citation_classifications``
and ``openneuro_paper_citation_classifications`` tables.

Trigger manually with configurable params (model, limits, dry-run, etc.)
via the Airflow UI.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:  # pragma: no cover
    from airflow.operators.python import PythonOperator  # type: ignore

try:
    from airflow.sdk import Param  # Airflow 3+
except Exception:  # pragma: no cover
    try:
        from airflow.models.param import Param  # Airflow 2.x
    except Exception:  # pragma: no cover
        Param = None  # type: ignore

try:
    from airflow.models.xcom_arg import XComArg
except Exception:  # pragma: no cover
    XComArg = None  # type: ignore

try:
    from utils.database import get_db_connection
    from utils.llm_classify import (
        get_openrouter_api_key,
        validate_openrouter_api_key,
        call_openrouter,
        build_classification_prompt,
        build_primary_relationship_prompt,
        normalize_confidence,
        normalize_openrouter_model,
        DEFAULT_MODEL,
        VALID_CLASSIFICATIONS,
    )
except ImportError:
    from dags.utils.database import get_db_connection
    from dags.utils.llm_classify import (
        get_openrouter_api_key,
        validate_openrouter_api_key,
        call_openrouter,
        build_classification_prompt,
        build_primary_relationship_prompt,
        normalize_confidence,
        normalize_openrouter_model,
        DEFAULT_MODEL,
        VALID_CLASSIFICATIONS,
    )

logger = logging.getLogger(__name__)

# Same repository labels as unified_datasets.source (dandi_dataset, openneuro_dataset, neuroscience_datasets).
DATASET_REPOSITORY_SOURCES: Tuple[str, ...] = ("DANDI", "OpenNeuro", "CRCNS", "Kaggle", "PhysioNet")
SOURCE_FILTER_ENUM: Tuple[str, ...] = ("all",) + DATASET_REPOSITORY_SOURCES

# (internal_key, citations_table, classifications_table, dataset_id_column)
_DANDI_ROW = ("dandi", "dandi_paper_citations", "dandi_paper_citation_classifications", "dandi_id")
_OPENNEURO_ROW = ("openneuro", "openneuro_paper_citations", "openneuro_paper_citation_classifications", "openneuro_id")
_CRCNS_ROW = ("crcns", "crcns_paper_citations", "crcns_paper_citation_classifications", "crcns_id")


def _normalize_repository_filter(raw: Any) -> str:
    """Map param value to canonical repository name or 'all'."""
    if raw is None:
        return "all"
    s = str(raw).strip()
    if not s:
        return "all"
    lower = s.lower()
    if lower == "all":
        return "all"
    aliases = {
        "dandi": "DANDI",
        "openneuro": "OpenNeuro",
        "crcns": "CRCNS",
        "kaggle": "Kaggle",
        "physionet": "PhysioNet",
    }
    if lower in aliases:
        return aliases[lower]
    if s in DATASET_REPOSITORY_SOURCES:
        return s
    logger.warning("Unknown source_filter %r, defaulting to 'all'", raw)
    return "all"


def _citation_table_sources_for_filter(canonical: str) -> List[Tuple[str, str, str, str]]:
    """Repositories that have paper citation + classification tables (subset of DATASET_REPOSITORY_SOURCES)."""
    if canonical == "all":
        return [_DANDI_ROW, _OPENNEURO_ROW, _CRCNS_ROW]
    if canonical == "DANDI":
        return [_DANDI_ROW]
    if canonical == "OpenNeuro":
        return [_OPENNEURO_ROW]
    if canonical == "CRCNS":
        return [_CRCNS_ROW]
    return []


# ---------------------------------------------------------------------------
# Task 1: Fetch unclassified edges
# ---------------------------------------------------------------------------

def _preflight_api_key_check(dry_run: bool) -> None:
    """Fail fast if dry_run is off and the OpenRouter API key is missing or rejected."""
    if dry_run:
        return
    key = get_openrouter_api_key()
    logger.info("OPENROUTER_API_KEY present (%d chars)", len(key))
    validate_openrouter_api_key(key)


def fetch_unclassified_edges(**context) -> List[Dict[str, Any]]:
    """Query citation edges that need classification."""
    params = context["params"]
    dry_run = bool(params.get("dry_run", False))
    _preflight_api_key_check(dry_run)

    max_edges = int(params.get("max_edges_per_run", 100))
    repo = _normalize_repository_filter(params.get("source_filter"))
    scope = str(params.get("classification_scope", "citation_edges")).lower()
    reclassify = bool(params.get("reclassify_existing", False))

    edges: List[Dict[str, Any]] = []

    sources = _citation_table_sources_for_filter(repo)
    if not sources:
        logger.info(
            "source_filter=%s: no paper citation / classification tables for this repository "
            "(classification is only wired for DANDI, OpenNeuro, and CRCNS). Returning 0 edges.",
            repo,
        )
        return []

    with get_db_connection() as conn:
        cursor = conn.cursor()

        for source_name, cit_table, cls_table, id_col in sources:
            if max_edges > 0 and len(edges) >= max_edges:
                break

            remaining = max_edges - len(edges) if max_edges > 0 else 10000

            if scope in ("citation_edges", "both"):
                edges.extend(
                    _fetch_citation_edge_candidates(
                        cursor, source_name, cit_table, cls_table, id_col,
                        remaining, reclassify, keys_only=True,
                    )
                )

            if scope in ("primary_only", "both") and (max_edges <= 0 or len(edges) < max_edges):
                remaining = max_edges - len(edges) if max_edges > 0 else 10000
                edges.extend(
                    _fetch_primary_candidates(
                        cursor, source_name, cit_table, cls_table, id_col,
                        remaining, reclassify, keys_only=True,
                    )
                )

    if max_edges > 0:
        edges = edges[:max_edges]

    logger.info(
        "Fetched %d edges to classify (source_filter=%s, scope=%s, reclassify=%s)",
        len(edges), repo, scope, reclassify,
    )
    return edges


def _fetch_citation_edge_candidates(
    cursor, source_name, cit_table, cls_table, id_col,
    limit, reclassify, keys_only=False,
) -> List[Dict[str, Any]]:
    """Fetch citation edges with contexts but missing / placeholder classification.

    When *keys_only* is True only composite-key columns are returned (no JSONB
    contexts or paper metadata) so the result is lightweight enough for XCom.
    """
    if reclassify:
        status_filter = ""
    else:
        status_filter = (
            f"AND (cls.id IS NULL OR cls.status IN ('placeholder', 'error'))"
        )

    if keys_only:
        select_clause = f"""
            cit.{id_col}           AS dataset_id,
            cit.primary_paper_doi,
            cit.citing_paper_doi"""
        join_papers = ""
    else:
        select_clause = f"""
            cit.{id_col}           AS dataset_id,
            cit.primary_paper_doi,
            cit.citing_paper_doi,
            cit.citation_contexts,
            pp.title               AS primary_title,
            pp.authors             AS primary_authors,
            cp.title               AS citing_title,
            cp.authors             AS citing_authors"""
        join_papers = (
            "LEFT JOIN papers pp ON pp.paper_doi = cit.primary_paper_doi\n"
            "        LEFT JOIN papers cp ON cp.paper_doi = cit.citing_paper_doi"
        )

    sql = f"""
        SELECT {select_clause}
        FROM {cit_table} cit
        LEFT JOIN {cls_table} cls
            ON  cls.{id_col}           = cit.{id_col}
            AND cls.primary_paper_doi  = cit.primary_paper_doi
            AND cls.citing_paper_doi   = cit.citing_paper_doi
        {join_papers}
        WHERE cit.citation_contexts IS NOT NULL
          AND cit.citation_contexts != 'null'::jsonb
          AND cit.citation_contexts != '[]'::jsonb
          {status_filter}
        ORDER BY cit.resolved_at DESC
        LIMIT %s;
    """
    cursor.execute(sql, (limit,))
    cols = [d[0] for d in cursor.description]
    rows = []
    for row in cursor.fetchall():
        d = dict(zip(cols, row))
        d["source"] = source_name
        d["edge_type"] = "citation_edge"
        if not keys_only and isinstance(d.get("citation_contexts"), str):
            try:
                d["citation_contexts"] = json.loads(d["citation_contexts"])
            except (json.JSONDecodeError, TypeError):
                d["citation_contexts"] = []
        rows.append(d)
    return rows


def _fetch_primary_candidates(
    cursor, source_name, cit_table, cls_table, id_col,
    limit, reclassify, keys_only=False,
) -> List[Dict[str, Any]]:
    """
    Fetch primary paper -> dataset edges.

    For primary classification we look at all citation edges for a given
    (dataset, primary_paper_doi) and aggregate their contexts.

    When *keys_only* is True only the composite key is returned (lightweight).
    """
    if reclassify:
        status_filter = ""
    else:
        status_filter = (
            "AND (cls.id IS NULL OR cls.status IN ('placeholder', 'error'))"
        )

    if keys_only:
        select_clause = f"""
            cit.{id_col}           AS dataset_id,
            cit.primary_paper_doi"""
        join_papers = ""
    else:
        select_clause = f"""
            cit.{id_col}           AS dataset_id,
            cit.primary_paper_doi,
            pp.title               AS primary_title,
            pp.authors             AS primary_authors"""
        join_papers = "LEFT JOIN papers pp ON pp.paper_doi = cit.primary_paper_doi"

    sql = f"""
        SELECT DISTINCT ON (cit.{id_col}, cit.primary_paper_doi)
            {select_clause}
        FROM {cit_table} cit
        LEFT JOIN {cls_table} cls
            ON  cls.{id_col}           = cit.{id_col}
            AND cls.primary_paper_doi  = cit.primary_paper_doi
            AND cls.citing_paper_doi   = cit.primary_paper_doi
        {join_papers}
        WHERE 1=1
          {status_filter}
        ORDER BY cit.{id_col}, cit.primary_paper_doi
        LIMIT %s;
    """
    cursor.execute(sql, (limit,))
    cols = [d[0] for d in cursor.description]
    rows = []
    for row in cursor.fetchall():
        d = dict(zip(cols, row))
        d["source"] = source_name
        d["edge_type"] = "primary"
        d["citing_paper_doi"] = d["primary_paper_doi"]
        if not keys_only:
            d["citing_title"] = d["primary_title"]
            d["citing_authors"] = d["primary_authors"]
            d["citation_contexts"] = []
        rows.append(d)
    return rows


# ---------------------------------------------------------------------------
# Re-fetch full edge data inside mapped tasks (avoids large XCom payloads)
# ---------------------------------------------------------------------------

def _fetch_full_edge_data(
    cursor,
    edge_keys: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Re-fetch full edge data (contexts, paper metadata) for a batch of lightweight keys.

    Groups keys by (source, edge_type) and issues one query per group against
    the appropriate citation/papers tables.  Returns dicts in the same shape
    that ``classify_and_persist_batch`` expects.
    """
    # Group keys by (source, edge_type)
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for key in edge_keys:
        g = (key["source"], key.get("edge_type", "citation_edge"))
        groups.setdefault(g, []).append(key)

    results: List[Dict[str, Any]] = []

    for (source, edge_type), keys in groups.items():
        if source == "dandi":
            cit_table = "dandi_paper_citations"
            id_col = "dandi_id"
        elif source == "openneuro":
            cit_table = "openneuro_paper_citations"
            id_col = "openneuro_id"
        elif source == "crcns":
            cit_table = "crcns_paper_citations"
            id_col = "crcns_id"
        else:
            logger.warning("_fetch_full_edge_data: unknown source %r, skipping %d keys", source, len(keys))
            continue

        if edge_type == "citation_edge":
            tuples = [
                (k["dataset_id"], k["primary_paper_doi"], k["citing_paper_doi"])
                for k in keys
            ]
            sql = f"""
                SELECT
                    cit.{id_col}           AS dataset_id,
                    cit.primary_paper_doi,
                    cit.citing_paper_doi,
                    cit.citation_contexts,
                    pp.title               AS primary_title,
                    pp.authors             AS primary_authors,
                    cp.title               AS citing_title,
                    cp.authors             AS citing_authors
                FROM {cit_table} cit
                LEFT JOIN papers pp ON pp.paper_doi = cit.primary_paper_doi
                LEFT JOIN papers cp ON cp.paper_doi = cit.citing_paper_doi
                WHERE ({id_col}, primary_paper_doi, citing_paper_doi) IN %s;
            """
            cursor.execute(sql, (tuple(tuples),))
            cols = [d[0] for d in cursor.description]
            for row in cursor.fetchall():
                d = dict(zip(cols, row))
                d["source"] = source
                d["edge_type"] = "citation_edge"
                if isinstance(d.get("citation_contexts"), str):
                    try:
                        d["citation_contexts"] = json.loads(d["citation_contexts"])
                    except (json.JSONDecodeError, TypeError):
                        d["citation_contexts"] = []
                results.append(d)

        elif edge_type == "primary":
            dois = tuple({k["primary_paper_doi"] for k in keys})
            sql = """
                SELECT paper_doi AS primary_paper_doi,
                       title     AS primary_title,
                       authors   AS primary_authors
                FROM papers
                WHERE paper_doi IN %s;
            """
            cursor.execute(sql, (dois,))
            cols = [d[0] for d in cursor.description]
            paper_lookup = {}
            for row in cursor.fetchall():
                d = dict(zip(cols, row))
                paper_lookup[d["primary_paper_doi"]] = d

            for k in keys:
                pdoi = k["primary_paper_doi"]
                pdata = paper_lookup.get(pdoi, {})
                results.append({
                    "dataset_id": k["dataset_id"],
                    "source": source,
                    "edge_type": "primary",
                    "primary_paper_doi": pdoi,
                    "citing_paper_doi": pdoi,
                    "primary_title": pdata.get("primary_title"),
                    "primary_authors": pdata.get("primary_authors"),
                    "citing_title": pdata.get("primary_title"),
                    "citing_authors": pdata.get("primary_authors"),
                    "citation_contexts": [],
                })

    if len(results) < len(edge_keys):
        logger.warning(
            "_fetch_full_edge_data: requested %d edges, got %d back (some may have been deleted)",
            len(edge_keys), len(results),
        )

    return results


# ---------------------------------------------------------------------------
# Task 2: Build batches
# ---------------------------------------------------------------------------

def build_classification_batches(**context) -> List[Dict[str, Any]]:
    """Split lightweight edge keys into batches for dynamic task mapping."""
    params = context["params"]
    batch_size = int(params.get("batch_size", 10))

    ti = context["ti"]
    edge_keys = ti.xcom_pull(task_ids="fetch_unclassified_edges") or []

    if not edge_keys:
        logger.info("No edges to classify — nothing to do.")
        return []

    batches = []
    for i in range(0, len(edge_keys), batch_size):
        chunk = edge_keys[i : i + batch_size]
        batches.append({"batch_edge_keys": chunk, "batch_index": len(batches)})

    logger.info("Created %d batches of up to %d edges each (%d total)", len(batches), batch_size, len(edge_keys))
    return batches


# ---------------------------------------------------------------------------
# Task 3: Classify and persist (mapped)
# ---------------------------------------------------------------------------

def classify_and_persist_batch(*, batch_edge_keys: List[Dict[str, Any]], batch_index: int, **context):
    """Classify a batch of edges and write results to Postgres."""
    params = context["params"]
    model = normalize_openrouter_model(str(params.get("model", DEFAULT_MODEL)))
    dry_run = bool(params.get("dry_run", False))
    max_retries = int(params.get("max_retries", 3))
    temperature = float(params.get("temperature", 0.1))
    max_tokens = int(params.get("max_tokens", 512))
    interval = float(params.get("min_api_interval_seconds", 0.5))
    run_id = context.get("run_id", "unknown")

    # Lazy-load API key only when a real OpenRouter call is needed (not dry_run, not all-skipped).
    api_key: str | None = None

    stats = {"classified": 0, "errors": 0, "skipped": 0}
    results_by_category: Dict[str, int] = {}
    usage_totals = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Re-fetch full edge data (contexts, paper metadata) from DB for this batch.
        batch_edges = _fetch_full_edge_data(cursor, batch_edge_keys)

        for idx, edge in enumerate(batch_edges):
            dataset_id = edge["dataset_id"]
            source = edge["source"]
            edge_type = edge.get("edge_type", "citation_edge")
            primary_doi = edge["primary_paper_doi"]
            citing_doi = edge["citing_paper_doi"]
            contexts = edge.get("citation_contexts") or []

            if not contexts and edge_type == "citation_edge":
                logger.warning(
                    "Skipping edge %s/%s->%s: no citation contexts",
                    dataset_id, primary_doi, citing_doi,
                )
                stats["skipped"] += 1
                continue

            # Build prompt
            if edge_type == "primary":
                prompt = build_primary_relationship_prompt(
                    dataset_id=dataset_id,
                    paper_title=edge.get("primary_title"),
                    paper_authors=_extract_author_names(edge.get("primary_authors")),
                    citation_contexts=contexts,
                )
            else:
                prompt = build_classification_prompt(
                    dataset_id=dataset_id,
                    primary_paper_title=edge.get("primary_title"),
                    citing_paper_title=edge.get("citing_title"),
                    citing_authors=_extract_author_names(edge.get("citing_authors")),
                    citation_contexts=contexts,
                )

            if dry_run:
                logger.info(
                    "[DRY RUN] batch=%d edge=%d/%d %s/%s->%s prompt_len=%d",
                    batch_index, idx + 1, len(batch_edges),
                    dataset_id, primary_doi, citing_doi, len(prompt),
                )
                logger.debug("[DRY RUN] Prompt:\n%s", prompt[:1500])
                result = {
                    "classification": "UNKNOWN",
                    "confidence": 1,
                    "reasoning": "dry_run — no API call made",
                }
                status = "dry_run"
            else:
                try:
                    if api_key is None:
                        api_key = get_openrouter_api_key()
                    result = call_openrouter(
                        prompt=prompt,
                        api_key=api_key,
                        model=model,
                        max_retries=max_retries,
                        max_tokens=max_tokens,
                        temperature=temperature,
                    )
                    status = "error" if result.get("parse_error") else "classified"
                    usage_totals["prompt_tokens"] += int(result.get("prompt_tokens") or 0)
                    usage_totals["completion_tokens"] += int(result.get("completion_tokens") or 0)
                    usage_totals["total_tokens"] += int(result.get("total_tokens") or 0)
                except Exception as exc:
                    logger.error(
                        "API error for %s/%s->%s: %s",
                        dataset_id, primary_doi, citing_doi, exc,
                    )
                    result = {
                        "classification": "UNKNOWN",
                        "confidence": 1,
                        "reasoning": f"API error: {exc}",
                    }
                    status = "error"

            classification = result.get("classification", "UNKNOWN").upper()
            if classification not in VALID_CLASSIFICATIONS:
                classification = "UNKNOWN"
            confidence = result.get("confidence", 1)
            if isinstance(confidence, str):
                confidence = normalize_confidence(confidence)
            reasoning = result.get("reasoning", "")

            # Upsert into the appropriate classification table
            _upsert_classification(
                cursor, source, dataset_id, primary_doi, citing_doi,
                classification, confidence, reasoning, model, status, run_id,
            )

            cat = classification
            results_by_category[cat] = results_by_category.get(cat, 0) + 1
            if status == "error":
                stats["errors"] += 1
            else:
                stats["classified"] += 1

            # Throttle between API calls
            if not dry_run and idx < len(batch_edges) - 1:
                time.sleep(interval)

        conn.commit()

    logger.info(
        "Batch %d done: %d classified, %d errors, %d skipped. Breakdown: %s. "
        "Tokens: prompt=%d completion=%d total=%d",
        batch_index, stats["classified"], stats["errors"], stats["skipped"],
        json.dumps(results_by_category),
        usage_totals["prompt_tokens"],
        usage_totals["completion_tokens"],
        usage_totals["total_tokens"],
    )

    return {
        "batch_index": batch_index,
        "stats": stats,
        "by_category": results_by_category,
        "usage": usage_totals,
    }


def _extract_author_names(authors_raw: Any) -> List[str]:
    """Normalise authors JSONB (list of strings or list of dicts) to a list of name strings."""
    if not authors_raw:
        return []
    if isinstance(authors_raw, str):
        try:
            authors_raw = json.loads(authors_raw)
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(authors_raw, list):
        out = []
        for a in authors_raw:
            if isinstance(a, str):
                out.append(a)
            elif isinstance(a, dict):
                name = a.get("name") or a.get("display_name") or ""
                if name:
                    out.append(name)
        return out
    return []


def _upsert_classification(
    cursor, source, dataset_id, primary_doi, citing_doi,
    classification, confidence, reasoning, model, status, run_id,
):
    """UPSERT a row into the source-appropriate classification table."""
    if source == "dandi":
        table = "dandi_paper_citation_classifications"
        id_col = "dandi_id"
    elif source == "openneuro":
        table = "openneuro_paper_citation_classifications"
        id_col = "openneuro_id"
    elif source == "crcns":
        table = "crcns_paper_citation_classifications"
        id_col = "crcns_id"
    else:
        logger.error("Unknown source %r, cannot upsert classification", source)
        return

    sql = f"""
        INSERT INTO {table} (
            {id_col}, primary_paper_doi, citing_paper_doi,
            classification, confidence, reasoning,
            classification_model, classified_at, status, run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
        ON CONFLICT ({id_col}, primary_paper_doi, citing_paper_doi) DO UPDATE SET
            classification       = EXCLUDED.classification,
            confidence           = EXCLUDED.confidence,
            reasoning            = EXCLUDED.reasoning,
            classification_model = EXCLUDED.classification_model,
            classified_at        = EXCLUDED.classified_at,
            status               = EXCLUDED.status,
            run_id               = EXCLUDED.run_id;
    """
    cursor.execute(sql, (
        dataset_id, primary_doi, citing_doi,
        classification, confidence, reasoning,
        model, status, run_id,
    ))


# ---------------------------------------------------------------------------
# Task 4: Summarize
# ---------------------------------------------------------------------------

def summarize_classification_run(**context):
    """Log aggregate stats for the run."""
    ti = context["ti"]
    batch_results = ti.xcom_pull(task_ids="classify_and_persist_batch") or []

    if not batch_results:
        logger.info("Classification run complete — no batches were processed.")
        return

    if isinstance(batch_results, dict):
        batch_results = [batch_results]

    total_classified = 0
    total_errors = 0
    total_skipped = 0
    category_totals: Dict[str, int] = {}
    sum_prompt = 0
    sum_completion = 0
    sum_total = 0

    for br in batch_results:
        if not isinstance(br, dict):
            continue
        s = br.get("stats", {})
        total_classified += s.get("classified", 0)
        total_errors += s.get("errors", 0)
        total_skipped += s.get("skipped", 0)
        for cat, count in br.get("by_category", {}).items():
            category_totals[cat] = category_totals.get(cat, 0) + count
        u = br.get("usage") or {}
        sum_prompt += int(u.get("prompt_tokens") or 0)
        sum_completion += int(u.get("completion_tokens") or 0)
        sum_total += int(u.get("total_tokens") or 0)

    model = normalize_openrouter_model(str(context["params"].get("model", DEFAULT_MODEL)))
    logger.info(
        "=== Classification Run Summary ===\n"
        "  Model:              %s\n"
        "  Classified:         %d\n"
        "  Errors:             %d\n"
        "  Skipped:            %d\n"
        "  Prompt tokens:      %d\n"
        "  Completion tokens:  %d\n"
        "  Total tokens:       %d\n"
        "  By category:        %s",
        model, total_classified, total_errors, total_skipped,
        sum_prompt, sum_completion, sum_total,
        json.dumps(category_totals, indent=2),
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

def _build_dag_params() -> Dict[str, Any]:
    """Default params; source_filter uses enum aligned with unified_datasets repositories."""
    p: Dict[str, Any] = {
        "model": DEFAULT_MODEL,
        "max_edges_per_run": 100,
        "batch_size": 10,
        "classification_scope": "citation_edges",
        "max_retries": 3,
        "temperature": 0.1,
        "max_tokens": 512,
        "min_api_interval_seconds": 0.5,
        "dry_run": False,
        "reclassify_existing": False,
    }
    if Param is not None:
        p["source_filter"] = Param(
            "all",
            title="Source repository",
            description=(
                "Same values as unified_datasets.source (ingested dataset repositories). "
                "LLM classification uses citation tables for DANDI, OpenNeuro, and CRCNS; "
                "choosing Kaggle or PhysioNet selects no edges until those pipelines exist."
            ),
            schema={"type": "string", "enum": list(SOURCE_FILTER_ENUM)},
        )
    else:
        p["source_filter"] = "all"
    return p


dag = DAG(
    "paper_reuse_classification",
    default_args={
        "owner": "neurod3",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="LLM-based classification of paper reuse via OpenRouter",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["datasets", "papers", "classification", "llm"],
    params=_build_dag_params(),
)

fetch_task = PythonOperator(
    task_id="fetch_unclassified_edges",
    python_callable=fetch_unclassified_edges,
    dag=dag,
)

build_batches_task = PythonOperator(
    task_id="build_classification_batches",
    python_callable=build_classification_batches,
    dag=dag,
)

if XComArg is None:  # pragma: no cover
    raise RuntimeError(
        "Dynamic task mapping requires XComArg (airflow.models.xcom_arg). "
        "Upgrade Airflow or update imports to enable mapped batch execution."
    )

classify_task = (
    PythonOperator.partial(
        task_id="classify_and_persist_batch",
        python_callable=classify_and_persist_batch,
        pool="dandi_paper_api_pool",
        dag=dag,
    ).expand(op_kwargs=XComArg(build_batches_task))
)

summarize_task = PythonOperator(
    task_id="summarize_classification_run",
    python_callable=summarize_classification_run,
    dag=dag,
)

fetch_task >> build_batches_task >> classify_task >> summarize_task
