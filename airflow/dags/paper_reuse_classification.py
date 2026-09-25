"""
Paper reuse classification DAG.

For every (citing paper, dataset) pair produced by the ``*_paper_mapping`` DAGs,
hand the model the WHOLE paper and ask whether the paper reused the dataset's
data. The classifier is ``utils/classify_fulltext_reuse.py`` (vendored from
catalystneuro/find_reuse); it returns a label plus verbatim quotes that are
checked against the paper text.

Labels by mode (``classification_scope`` param):
  citation_edges -> mode ``citing``:  REUSE | MENTION | NEITHER
  primary_only   -> mode ``direct``:  PRIMARY | REUSE | NEITHER
  both           -> run both

Row statuses: ``classified``, ``error`` (transport/parse failure; never
overwrites a good row), ``no_full_text`` (body not retrievable yet; retried on
a later run), ``placeholder`` (created by the mapping DAGs). A ``dry_run``
writes no rows at all; it only logs prompt sizes and text availability.

Results go to ``<src>_paper_citation_classifications``; every run writes one
row to ``paper_reuse_classification_runs``. Trigger manually from the Airflow
UI; see the README section "Paper Reuse Classification".
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from airflow import DAG
from airflow.exceptions import AirflowFailException
from psycopg2.extras import Json

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
    from utils.database import get_db_connection, ensure_paper_reuse_classification_columns
    from utils.llm_classify import get_openrouter_api_key, validate_openrouter_api_key
    from utils.classify_fulltext_reuse import (
        classify_paper_reuse,
        build_prompt,
        openrouter_credit_remaining,
        PROMPT_VERSION,
        MODE_CITING,
        MODE_DIRECT,
        DEFAULT_MODEL,
        DEFAULT_REASONING_EFFORT,
        VALID_REASONING_EFFORTS,
        DEFAULT_MAX_TOKENS,
        DEFAULT_MAX_INPUT_CHARS,
    )
    from utils.paper_fulltext import (
        load_cached_paper_text,
        fetch_fulltext_detailed,
        fetcher_cache_key,
        TEXT_STATUS_FULL,
    )
    from utils.find_reuse_core import Telemetry
except ImportError:  # pragma: no cover - direct import outside the dags folder
    from dags.utils.database import get_db_connection, ensure_paper_reuse_classification_columns
    from dags.utils.llm_classify import get_openrouter_api_key, validate_openrouter_api_key
    from dags.utils.classify_fulltext_reuse import (
        classify_paper_reuse,
        build_prompt,
        openrouter_credit_remaining,
        PROMPT_VERSION,
        MODE_CITING,
        MODE_DIRECT,
        DEFAULT_MODEL,
        DEFAULT_REASONING_EFFORT,
        VALID_REASONING_EFFORTS,
        DEFAULT_MAX_TOKENS,
        DEFAULT_MAX_INPUT_CHARS,
    )
    from dags.utils.paper_fulltext import (
        load_cached_paper_text,
        fetch_fulltext_detailed,
        fetcher_cache_key,
        TEXT_STATUS_FULL,
    )
    from dags.utils.find_reuse_core import Telemetry

logger = logging.getLogger(__name__)

POOL_NAME = "paper_reuse_classification_pool"

# Same repository labels as unified_datasets.source.
DATASET_REPOSITORY_SOURCES: Tuple[str, ...] = ("DANDI", "OpenNeuro", "CRCNS", "SPARC", "Kaggle", "PhysioNet")
SOURCE_FILTER_ENUM: Tuple[str, ...] = ("all",) + DATASET_REPOSITORY_SOURCES

# (internal_key, citations_table, classifications_table, dataset_id_column)
_DANDI_ROW = ("dandi", "dandi_paper_citations", "dandi_paper_citation_classifications", "dandi_id")
_OPENNEURO_ROW = ("openneuro", "openneuro_paper_citations", "openneuro_paper_citation_classifications", "openneuro_id")
_CRCNS_ROW = ("crcns", "crcns_paper_citations", "crcns_paper_citation_classifications", "crcns_id")
_SPARC_ROW = ("sparc", "sparc_paper_citations", "sparc_paper_citation_classifications", "sparc_id")
_SOURCE_ROWS = {row[0]: row for row in (_DANDI_ROW, _OPENNEURO_ROW, _CRCNS_ROW, _SPARC_ROW)}

# internal_key -> unified_datasets.source label and per-source dataset table
_UNIFIED_SOURCE_LABEL = {"dandi": "DANDI", "openneuro": "OpenNeuro", "crcns": "CRCNS", "sparc": "SPARC"}
_DATASET_TABLE = {"dandi": "dandi_dataset", "openneuro": "openneuro_dataset", "crcns": "crcns_dataset", "sparc": "sparc_dataset"}

STATUS_CLASSIFIED = "classified"
STATUS_ERROR = "error"
STATUS_NO_FULL_TEXT = "no_full_text"
STATUS_DRY_RUN = "dry_run"
STATUS_PLACEHOLDER = "placeholder"

# Statuses that mean "this row does not hold a real judgement". A row in one
# of these states may be overwritten by anything; a row outside them may only
# be overwritten by a real classification.
_NON_RESULT_STATUSES = (STATUS_ERROR, STATUS_PLACEHOLDER, STATUS_NO_FULL_TEXT, STATUS_DRY_RUN)
_NON_RESULT_STATUS_SQL = "(" + ", ".join(f"'{s}'" for s in _NON_RESULT_STATUSES) + ")"


# ---------------------------------------------------------------------------
# Small pure helpers (unit-tested in dags/tests/test_paper_reuse_classification.py)
# ---------------------------------------------------------------------------

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
        "sparc": "SPARC",
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
    """Repositories that have paper citation + classification tables."""
    if canonical == "all":
        return [_DANDI_ROW, _OPENNEURO_ROW, _CRCNS_ROW, _SPARC_ROW]
    for row in _SOURCE_ROWS.values():
        if _UNIFIED_SOURCE_LABEL[row[0]] == canonical:
            return [row]
    return []


def _resolve_model(raw: Any) -> str:
    return (str(raw) if raw is not None else "").strip() or DEFAULT_MODEL


def _resolve_reasoning_effort(raw: Any) -> Optional[str]:
    """'none' / '' disable the field; anything else must be a valid effort."""
    s = (str(raw) if raw is not None else "").strip().lower()
    if not s or s == "none":
        return None
    if s not in VALID_REASONING_EFFORTS:
        raise AirflowFailException(
            f"reasoning_effort={raw!r} is not one of {sorted(VALID_REASONING_EFFORTS)} or 'none'"
        )
    return s


def _mode_for_edge_type(edge_type: str) -> str:
    return MODE_DIRECT if edge_type == "primary" else MODE_CITING


def _candidate_status_filter(reclassify: bool, prompt_version: int, model: str) -> Tuple[str, Dict[str, Any]]:
    """
    SQL predicate (and its named params) selecting rows that still need work.

    A row needs work when it has never been classified, its last attempt left
    no real judgement, or it was judged under a different prompt version or
    model. ``reclassify`` selects everything.
    """
    if reclassify:
        return "", {}
    sql = (
        "AND (cls.id IS NULL\n"
        f"     OR cls.status IN {_NON_RESULT_STATUS_SQL}\n"
        "     OR cls.prompt_version IS DISTINCT FROM %(prompt_version)s\n"
        "     OR cls.classification_model IS DISTINCT FROM %(model)s)"
    )
    return sql, {"prompt_version": prompt_version, "model": model}


def _citation_edge_order_sql(mix_publishers: bool) -> str:
    """
    ORDER BY for citation-edge candidates.

    Always: never-classified rows first, then other rows needing work, then
    rows waiting on full text. Within that, by default, citing DOI order
    (alphabetical, so a small run sees one publisher: 10.1002 is Wiley).
    ``mix_publishers`` instead deals round-robin across DOI prefixes (one
    publisher's pair, then the next publisher's, ...), each prefix in a stable
    hash order, so a small run is a mixed sample and reruns continue it.
    """
    priority = (
        "CASE WHEN cls.id IS NULL THEN 0 "
        f"WHEN cls.status = '{STATUS_NO_FULL_TEXT}' THEN 2 "
        "ELSE 1 END"
    )
    if not mix_publishers:
        return f"{priority}, cit.citing_paper_doi, cit.resolved_at DESC"
    prefix = "split_part(cit.citing_paper_doi, '/', 1)"
    stable = "md5(cit.citing_paper_doi || '|' || cit.primary_paper_doi)"
    return (
        f"{priority}, "
        f"ROW_NUMBER() OVER (PARTITION BY ({priority}), {prefix} ORDER BY {stable}), "
        f"{prefix}, {stable}"
    )


def _group_edges_by_paper(edges: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """
    Group edge keys by (source, citing paper), preserving first-seen order.

    All of one paper's pairs are classified back to back on one worker: the
    prompt starts with the paper text, providers cache prompts by prefix, and
    the second and later questions about the same paper bill that text at the
    cached rate.
    """
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for e in edges:
        key = (e["source"], e["citing_paper_doi"])
        groups.setdefault(key, []).append(e)
    return list(groups.values())


def _pack_paper_groups(groups: List[List[Dict[str, Any]]], papers_per_batch: int) -> List[List[List[Dict[str, Any]]]]:
    """Pack whole paper groups into batches of at most ``papers_per_batch`` papers."""
    papers_per_batch = max(1, int(papers_per_batch))
    batches: List[List[List[Dict[str, Any]]]] = []
    current: List[List[Dict[str, Any]]] = []
    for g in groups:
        if len(current) >= papers_per_batch:
            batches.append(current)
            current = []
        current.append(g)
    if current:
        batches.append(current)
    return batches


def _result_to_row(result: Dict[str, Any], status: str, model: str, mode: str) -> Dict[str, Any]:
    """Map a classifier result dict onto the classification table's columns."""
    classification = result.get("classification")
    if classification == "ERROR":
        classification = None
    usage = result.get("usage")
    return {
        "classification": classification,
        "confidence": result.get("confidence"),
        "reasoning": result.get("reasoning") or result.get("error"),
        "classification_model": model,
        "status": status,
        "prompt_version": result.get("prompt_version", PROMPT_VERSION),
        "mode": result.get("mode") or mode,
        "reuse_type": result.get("reuse_type"),
        "reuse_type_other": result.get("reuse_type_other"),
        "reused_modalities": result.get("reused_modalities") or [],
        "reused_dandi_hosted": result.get("reused_dandi_hosted"),
        "reused_neurophysiology": result.get("reused_neurophysiology"),
        "same_lab": result.get("same_lab"),
        "same_lab_confidence": result.get("same_lab_confidence"),
        "source_archive": result.get("source_archive"),
        "evidence_quotes": result.get("evidence_quotes") or [],
        "source_quotes": result.get("source_quotes") or [],
        "quote_warnings": result.get("quote_warnings") or [],
        "hallucinated_quote_count": result.get("hallucinated_quote_count") or 0,
        "error_kind": result.get("error_kind"),
        "usage": usage if isinstance(usage, dict) else None,
        "input_chars": result.get("input_chars"),
        "truncation": result.get("truncation"),
        "provider": result.get("provider"),
    }


def _usage_numbers(usage: Optional[Dict[str, Any]]) -> Tuple[int, int, int, Optional[float]]:
    """(prompt_tokens, completion_tokens, total_tokens, cost or None) from a usage dict."""
    if not isinstance(usage, dict):
        return 0, 0, 0, None
    p = int(usage.get("prompt_tokens") or 0)
    c = int(usage.get("completion_tokens") or 0)
    t = int(usage.get("total_tokens") or (p + c))
    cost = usage.get("cost")
    try:
        cost = float(cost) if cost is not None else None
    except (TypeError, ValueError):
        cost = None
    return p, c, t, cost


# ---------------------------------------------------------------------------
# Task 0: schema
# ---------------------------------------------------------------------------

def create_classification_schema(**context) -> Dict[str, Any]:
    """Add the whole-paper classification columns and the runs table (idempotent)."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        touched = ensure_paper_reuse_classification_columns(cursor)
        conn.commit()
    logger.info("Classification schema ensured: %s", touched)
    return touched


# ---------------------------------------------------------------------------
# Task 1: fetch unclassified edges
# ---------------------------------------------------------------------------

def _public_table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table_name}",))
    row = cursor.fetchone()
    return bool(row and row[0])


def _drop_sources_with_missing_tables(cursor, sources):
    kept = []
    for row in sources:
        _src, cit_table, cls_table, _id = row
        if _public_table_exists(cursor, cit_table) and _public_table_exists(cursor, cls_table):
            kept.append(row)
        else:
            logger.info("Skipping %s: tables %s / %s not present.", row[0], cit_table, cls_table)
    return kept


def _preflight(params: Dict[str, Any]) -> None:
    """Fail fast on a missing/rejected key or an exhausted OpenRouter balance."""
    if bool(params.get("dry_run", False)):
        return
    key = get_openrouter_api_key()
    logger.info("OPENROUTER_API_KEY present (%d chars)", len(key))
    validate_openrouter_api_key(key)

    model = _resolve_model(params.get("model"))
    min_credit = float(params.get("min_credit_usd", 0) or 0)
    remaining = openrouter_credit_remaining(model)
    if remaining is None:
        logger.info("OpenRouter credit balance unknown (unlimited key or not an OpenRouter model); continuing.")
        return
    logger.info("OpenRouter credit remaining: $%.2f (floor $%.2f)", remaining, min_credit)
    if remaining < min_credit:
        raise AirflowFailException(
            f"OpenRouter credit ${remaining:.2f} is below min_credit_usd=${min_credit:.2f}; "
            "top up the key or lower the floor before classifying."
        )


def fetch_unclassified_edges(**context) -> List[Dict[str, Any]]:
    """Return lightweight keys of (dataset, primary paper, citing paper) pairs to classify."""
    params = context["params"]
    _preflight(params)

    max_edges = int(params.get("max_edges_per_run", 100))
    repo = _normalize_repository_filter(params.get("source_filter"))
    scope = str(params.get("classification_scope", "citation_edges")).lower()
    reclassify = bool(params.get("reclassify_existing", False))
    mix_publishers = bool(params.get("mix_publishers", False))
    model = _resolve_model(params.get("model"))

    sources = _citation_table_sources_for_filter(repo)
    if not sources:
        logger.info("source_filter=%s has no citation tables; returning 0 edges.", repo)
        return []

    edges: List[Dict[str, Any]] = []
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # The schema task runs first in the graph, but this task can also be
        # retried on its own, or run from a DAG version serialized before the
        # schema task existed. Ensuring here too is idempotent and cheap.
        ensure_paper_reuse_classification_columns(cursor)
        conn.commit()

        if repo == "all":
            sources = _drop_sources_with_missing_tables(cursor, sources)
            if not sources:
                logger.info("No citation / classification tables exist yet; run the paper-mapping DAGs first.")
                return []

        status_sql, status_params = _candidate_status_filter(reclassify, PROMPT_VERSION, model)

        for source_name, cit_table, cls_table, id_col in sources:
            if max_edges > 0 and len(edges) >= max_edges:
                break
            # None -> LIMIT NULL, which Postgres treats as no limit (max_edges_per_run=0).
            remaining = max_edges - len(edges) if max_edges > 0 else None

            if scope in ("citation_edges", "both"):
                edges.extend(_fetch_citation_edge_candidates(
                    cursor, source_name, cit_table, cls_table, id_col, remaining, status_sql, status_params,
                    mix_publishers=mix_publishers))

            if scope in ("primary_only", "both") and (max_edges <= 0 or len(edges) < max_edges):
                remaining = max_edges - len(edges) if max_edges > 0 else None
                edges.extend(_fetch_primary_candidates(
                    cursor, source_name, cit_table, cls_table, id_col, remaining, status_sql, status_params))

    if max_edges > 0:
        edges = edges[:max_edges]

    logger.info(
        "Fetched %d edges to classify (source_filter=%s, scope=%s, reclassify=%s, mix_publishers=%s, prompt_version=%s, model=%s)",
        len(edges), repo, scope, reclassify, mix_publishers, PROMPT_VERSION, model,
    )
    return edges


def _fetch_citation_edge_candidates(cursor, source_name, cit_table, cls_table, id_col, limit, status_sql, status_params,
                                    mix_publishers: bool = False):
    """Citation edges (citing paper cites the dataset's primary paper) -> mode citing."""
    sql = f"""
        SELECT cit.{id_col} AS dataset_id, cit.primary_paper_doi, cit.citing_paper_doi
        FROM {cit_table} cit
        LEFT JOIN {cls_table} cls
            ON  cls.{id_col}          = cit.{id_col}
            AND cls.primary_paper_doi = cit.primary_paper_doi
            AND cls.citing_paper_doi  = cit.citing_paper_doi
        WHERE 1=1
          {status_sql}
        ORDER BY
            {_citation_edge_order_sql(mix_publishers)}
        LIMIT %(limit)s;
    """
    cursor.execute(sql, {**status_params, "limit": limit})
    cols = [d[0] for d in cursor.description]
    rows = []
    for row in cursor.fetchall():
        d = dict(zip(cols, row))
        d["source"] = source_name
        d["edge_type"] = "citation_edge"
        rows.append(d)
    return rows


def _fetch_primary_candidates(cursor, source_name, cit_table, cls_table, id_col, limit, status_sql, status_params):
    """Primary paper -> dataset pairs (the paper names the dataset itself) -> mode direct."""
    sql = f"""
        SELECT DISTINCT ON (cit.{id_col}, cit.primary_paper_doi)
            cit.{id_col} AS dataset_id, cit.primary_paper_doi
        FROM {cit_table} cit
        LEFT JOIN {cls_table} cls
            ON  cls.{id_col}          = cit.{id_col}
            AND cls.primary_paper_doi = cit.primary_paper_doi
            AND cls.citing_paper_doi  = cit.primary_paper_doi
        WHERE 1=1
          {status_sql}
        ORDER BY cit.{id_col}, cit.primary_paper_doi
        LIMIT %(limit)s;
    """
    cursor.execute(sql, {**status_params, "limit": limit})
    cols = [d[0] for d in cursor.description]
    rows = []
    for row in cursor.fetchall():
        d = dict(zip(cols, row))
        d["source"] = source_name
        d["edge_type"] = "primary"
        d["citing_paper_doi"] = d["primary_paper_doi"]
        rows.append(d)
    return rows


# ---------------------------------------------------------------------------
# Task 2: batches (one paper's pairs never split across batches)
# ---------------------------------------------------------------------------

def build_classification_batches(**context) -> List[Dict[str, Any]]:
    params = context["params"]
    papers_per_batch = int(params.get("batch_size", 10))
    edge_keys = context["ti"].xcom_pull(task_ids="fetch_unclassified_edges") or []
    if not edge_keys:
        logger.info("No edges to classify; nothing to do.")
        return []
    groups = _group_edges_by_paper(edge_keys)
    batches = _pack_paper_groups(groups, papers_per_batch)
    logger.info(
        "Created %d batches of up to %d papers (%d papers, %d pairs)",
        len(batches), papers_per_batch, len(groups), len(edge_keys),
    )
    return [{"batch_paper_groups": b, "batch_index": i} for i, b in enumerate(batches)]


# ---------------------------------------------------------------------------
# Task 3: classify and persist (mapped)
# ---------------------------------------------------------------------------

class _DatasetLookup:
    """Dataset name/description from unified_datasets (or the per-source table), memoised."""

    def __init__(self, cursor):
        self.cursor = cursor
        self.cache: Dict[Tuple[str, str], Tuple[str, str]] = {}
        self.has_unified = _public_table_exists(cursor, "unified_datasets")

    def get(self, source: str, dataset_id: str) -> Tuple[str, str]:
        key = (source, dataset_id)
        if key in self.cache:
            return self.cache[key]
        name, desc = "", ""
        try:
            if self.has_unified:
                self.cursor.execute(
                    "SELECT title, COALESCE(full_description, description) FROM unified_datasets "
                    "WHERE source = %s AND dataset_id = %s LIMIT 1;",
                    (_UNIFIED_SOURCE_LABEL.get(source, source), dataset_id),
                )
            else:
                table = _DATASET_TABLE.get(source)
                if table and _public_table_exists(self.cursor, table):
                    self.cursor.execute(
                        f"SELECT title, description FROM {table} WHERE dataset_id = %s LIMIT 1;",
                        (dataset_id,),
                    )
                else:
                    self.cache[key] = (name, desc)
                    return name, desc
            row = self.cursor.fetchone()
            if row:
                name = row[0] or ""
                desc = row[1] or ""
        except Exception:
            logger.warning("Dataset lookup failed for %s/%s", source, dataset_id, exc_info=True)
        self.cache[key] = (name, desc)
        return name, desc


def _persist_fetch_result(cursor, citing_doi: str, detailed: Dict[str, Any]) -> None:
    cursor.execute(
        """
        UPDATE papers
        SET text_status = %s,
            fulltext_fetcher_source = %s,
            fulltext_fetcher_cache_key = COALESCE(%s, fulltext_fetcher_cache_key),
            fulltext_fetcher_fetched_at = NOW()
        WHERE paper_doi = %s;
        """,
        (detailed.get("status"), detailed.get("source"), fetcher_cache_key(citing_doi), citing_doi),
    )


def _mark_text_status_if_unknown(cursor, citing_doi: str, status: str) -> None:
    cursor.execute(
        "UPDATE papers SET text_status = COALESCE(text_status, %s) WHERE paper_doi = %s;",
        (status, citing_doi),
    )


def _upsert_classification(cursor, source: str, dataset_id: str, primary_doi: str, citing_doi: str,
                           row: Dict[str, Any], run_id: str) -> int:
    """
    UPSERT into the source's classification table.

    A non-result (error / no_full_text / dry_run) never replaces a row that
    holds a real judgement; it only fills empty or previously failed rows.
    Returns the number of rows written (0 when the guard blocked the update).
    """
    src = _SOURCE_ROWS.get(source)
    if src is None:
        logger.error("Unknown source %r, cannot upsert classification", source)
        return 0
    _key, _cit_table, table, id_col = src

    sql = f"""
        INSERT INTO {table} (
            {id_col}, primary_paper_doi, citing_paper_doi,
            classification, confidence, reasoning, classification_model, classified_at, status, run_id,
            prompt_version, mode, reuse_type, reuse_type_other, reused_modalities,
            reused_dandi_hosted, reused_neurophysiology,
            same_lab, same_lab_confidence, source_archive,
            evidence_quotes, source_quotes, quote_warnings, hallucinated_quote_count,
            error_kind, usage, input_chars, truncation, provider
        )
        VALUES (
            %(dataset_id)s, %(primary_doi)s, %(citing_doi)s,
            %(classification)s, %(confidence)s, %(reasoning)s, %(classification_model)s, NOW(), %(status)s, %(run_id)s,
            %(prompt_version)s, %(mode)s, %(reuse_type)s, %(reuse_type_other)s, %(reused_modalities)s,
            %(reused_dandi_hosted)s, %(reused_neurophysiology)s,
            %(same_lab)s, %(same_lab_confidence)s, %(source_archive)s,
            %(evidence_quotes)s, %(source_quotes)s, %(quote_warnings)s, %(hallucinated_quote_count)s,
            %(error_kind)s, %(usage)s, %(input_chars)s, %(truncation)s, %(provider)s
        )
        ON CONFLICT ({id_col}, primary_paper_doi, citing_paper_doi) DO UPDATE SET
            classification           = EXCLUDED.classification,
            confidence               = EXCLUDED.confidence,
            reasoning                = EXCLUDED.reasoning,
            classification_model     = EXCLUDED.classification_model,
            classified_at            = EXCLUDED.classified_at,
            status                   = EXCLUDED.status,
            run_id                   = EXCLUDED.run_id,
            prompt_version           = EXCLUDED.prompt_version,
            mode                     = EXCLUDED.mode,
            reuse_type               = EXCLUDED.reuse_type,
            reuse_type_other         = EXCLUDED.reuse_type_other,
            reused_modalities        = EXCLUDED.reused_modalities,
            reused_dandi_hosted      = EXCLUDED.reused_dandi_hosted,
            reused_neurophysiology   = EXCLUDED.reused_neurophysiology,
            same_lab                 = EXCLUDED.same_lab,
            same_lab_confidence      = EXCLUDED.same_lab_confidence,
            source_archive           = EXCLUDED.source_archive,
            evidence_quotes          = EXCLUDED.evidence_quotes,
            source_quotes            = EXCLUDED.source_quotes,
            quote_warnings           = EXCLUDED.quote_warnings,
            hallucinated_quote_count = EXCLUDED.hallucinated_quote_count,
            error_kind               = EXCLUDED.error_kind,
            usage                    = EXCLUDED.usage,
            input_chars              = EXCLUDED.input_chars,
            truncation               = EXCLUDED.truncation,
            provider                 = EXCLUDED.provider
        WHERE NOT (
            EXCLUDED.status IN ('{STATUS_ERROR}', '{STATUS_NO_FULL_TEXT}', '{STATUS_DRY_RUN}')
            AND {table}.status NOT IN {_NON_RESULT_STATUS_SQL}
        );
    """
    values = {
        "dataset_id": dataset_id,
        "primary_doi": primary_doi,
        "citing_doi": citing_doi,
        "run_id": run_id,
        **row,
        "reused_modalities": Json(row.get("reused_modalities") or []),
        "evidence_quotes": Json(row.get("evidence_quotes") or []),
        "source_quotes": Json(row.get("source_quotes") or []),
        "quote_warnings": Json(row.get("quote_warnings") or []),
        "usage": Json(row["usage"]) if row.get("usage") is not None else None,
        "truncation": Json(row["truncation"]) if row.get("truncation") is not None else None,
    }
    cursor.execute(sql, values)
    return cursor.rowcount


def _new_batch_stats() -> Dict[str, Any]:
    return {
        "pairs": 0, "papers": 0,
        "classified": 0, "errors": 0, "no_full_text": 0, "dry_run": 0, "guarded": 0,
        "text_from_cache": 0, "text_fetched_on_demand": 0,
        "hallucinated_quotes": 0,
        "by_category": {},
        "quote_tiers": {},
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0,
                  "cost_usd": 0.0, "cost_known_for": 0},
    }


def classify_and_persist_batch(*, batch_paper_groups: List[List[Dict[str, Any]]], batch_index: int, **context):
    """Classify every pair of every paper in this batch and write the rows."""
    params = context["params"]
    model = _resolve_model(params.get("model"))
    reasoning_effort = _resolve_reasoning_effort(params.get("reasoning_effort", DEFAULT_REASONING_EFFORT))
    dry_run = bool(params.get("dry_run", False))
    fetch_missing = bool(params.get("fetch_missing_fulltext", True))
    max_retries = int(params.get("max_retries", 3))
    temperature = float(params.get("temperature", 0.0))
    max_tokens = int(params.get("max_tokens", DEFAULT_MAX_TOKENS))
    max_input_chars = int(params.get("max_input_chars", DEFAULT_MAX_INPUT_CHARS))
    interval = float(params.get("min_api_interval_seconds", 0.5))
    run_id = context.get("run_id", "unknown")

    api_key: Optional[str] = None
    telemetry = Telemetry()
    stats = _new_batch_stats()
    fatal: Optional[str] = None

    with get_db_connection() as conn:
        cursor = conn.cursor()
        datasets = _DatasetLookup(cursor)

        for group in batch_paper_groups:
            if fatal:
                break
            citing_doi = group[0]["citing_paper_doi"]
            source = group[0]["source"]
            stats["papers"] += 1

            # 1) full text: disk cache first, then on demand
            text = load_cached_paper_text(cursor, citing_doi)
            if text:
                stats["text_from_cache"] += 1
                _mark_text_status_if_unknown(cursor, citing_doi, TEXT_STATUS_FULL)
            elif fetch_missing:
                detailed = fetch_fulltext_detailed(citing_doi, telemetry=telemetry)
                _persist_fetch_result(cursor, citing_doi, detailed)
                if detailed["status"] == TEXT_STATUS_FULL:
                    text = detailed["text"]
                    stats["text_fetched_on_demand"] += 1
                else:
                    logger.info("No full text for %s: %s (%s)", citing_doi, detailed["status"], detailed.get("reason"))

            for edge in group:
                stats["pairs"] += 1
                dataset_id = edge["dataset_id"]
                primary_doi = edge["primary_paper_doi"]
                mode = _mode_for_edge_type(edge.get("edge_type", "citation_edge"))

                if not text:
                    if dry_run:
                        stats["no_full_text"] += 1
                        continue
                    row = _result_to_row(
                        {"classification": "ERROR", "error_kind": STATUS_NO_FULL_TEXT,
                         "error": "article body not available", "mode": mode},
                        STATUS_NO_FULL_TEXT, model, mode)
                    if _upsert_classification(cursor, source, dataset_id, primary_doi, citing_doi, row, run_id) == 0:
                        stats["guarded"] += 1
                    stats["no_full_text"] += 1
                    continue

                dataset_name, dataset_description = datasets.get(source, dataset_id)

                if dry_run:
                    # Build the prompt (exercises the text load and the dataset
                    # lookup) but write nothing: a dry run must leave the tables
                    # and the dashboard's label breakdown exactly as they were.
                    prompt = build_prompt(text, dataset_id=dataset_id, dataset_name=dataset_name,
                                          dataset_description=dataset_description,
                                          primary_paper_doi=primary_doi, mode=mode)
                    logger.info("[DRY RUN] batch=%d %s/%s -> %s mode=%s prompt_chars=%d paper_chars=%d",
                                batch_index, source, dataset_id, citing_doi, mode, len(prompt), len(text))
                    stats["dry_run"] += 1
                    continue

                if api_key is None:
                    api_key = get_openrouter_api_key()
                result = classify_paper_reuse(
                    text,
                    dataset_id=dataset_id,
                    dataset_name=dataset_name,
                    dataset_description=dataset_description,
                    primary_paper_doi=primary_doi,
                    paper_doi=citing_doi,
                    api_key=api_key,
                    model=model,
                    max_input_chars=max_input_chars,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    max_retries=max_retries,
                    mode=mode,
                    reasoning_effort=reasoning_effort,
                )
                label = result.get("classification")
                status = STATUS_ERROR if label == "ERROR" else STATUS_CLASSIFIED
                row = _result_to_row(result, status, model, mode)
                written = _upsert_classification(cursor, source, dataset_id, primary_doi, citing_doi, row, run_id)
                if written == 0:
                    stats["guarded"] += 1

                if status == STATUS_ERROR:
                    stats["errors"] += 1
                    logger.warning("ERROR for %s/%s -> %s: %s (%s)", source, dataset_id, citing_doi,
                                   result.get("error_kind"), result.get("error"))
                    if result.get("fatal"):
                        fatal = f"{result.get('error_kind')}: {result.get('error')}"
                        break
                else:
                    stats["classified"] += 1
                    stats["by_category"][label] = stats["by_category"].get(label, 0) + 1
                    stats["hallucinated_quotes"] += int(result.get("hallucinated_quote_count") or 0)
                    for q in (result.get("evidence_quotes") or []):
                        tier = (q or {}).get("match_type") or "unknown"
                        stats["quote_tiers"][tier] = stats["quote_tiers"].get(tier, 0) + 1

                p, c, t, cost = _usage_numbers(result.get("usage"))
                stats["usage"]["prompt_tokens"] += p
                stats["usage"]["completion_tokens"] += c
                stats["usage"]["total_tokens"] += t
                if cost is not None:
                    stats["usage"]["cost_usd"] += cost
                    stats["usage"]["cost_known_for"] += 1

                if interval > 0:
                    time.sleep(interval)

        conn.commit()

    stats["telemetry"] = {
        "fetch_requests": telemetry.total_requests,
        "fetch_retries": telemetry.api_retry_count,
    }
    logger.info("Batch %d done: %s", batch_index, json.dumps(stats, default=str))

    if fatal:
        # Results so far are committed; the run stops here rather than burning
        # the rest of the corpus on a dead key.
        raise AirflowFailException(f"Fatal API error, aborting batch {batch_index}: {fatal}")

    return {"batch_index": batch_index, "stats": stats}


# ---------------------------------------------------------------------------
# Task 4: summarize
# ---------------------------------------------------------------------------

def _merge_stats(batch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = _new_batch_stats()
    total["batches"] = 0
    for br in batch_results:
        if not isinstance(br, dict) or not isinstance(br.get("stats"), dict):
            continue
        s = br["stats"]
        total["batches"] += 1
        for k in ("pairs", "papers", "classified", "errors", "no_full_text", "dry_run", "guarded",
                  "text_from_cache", "text_fetched_on_demand", "hallucinated_quotes"):
            total[k] += int(s.get(k) or 0)
        for k, v in (s.get("by_category") or {}).items():
            total["by_category"][k] = total["by_category"].get(k, 0) + v
        for k, v in (s.get("quote_tiers") or {}).items():
            total["quote_tiers"][k] = total["quote_tiers"].get(k, 0) + v
        u = s.get("usage") or {}
        for k in ("prompt_tokens", "completion_tokens", "total_tokens", "cost_known_for"):
            total["usage"][k] += int(u.get(k) or 0)
        total["usage"]["cost_usd"] += float(u.get("cost_usd") or 0.0)
    return total


def _failed_upstreams(edges: Any, batches: Any, batch_results: List[Any]) -> List[str]:
    """
    Name the upstream tasks that did not finish, judged from their XComs.

    The summary task runs on ``all_done`` so a run is always recorded, but a
    leaf that succeeds would make the whole DAG run read as success. A task
    that failed leaves no XCom (``None``), whereas one that ran and found
    nothing returns an empty list; a mapped batch that failed is simply
    missing from the results sequence.
    """
    failed: List[str] = []
    if edges is None:
        failed.append("fetch_unclassified_edges")
        return failed
    if batches is None:
        failed.append("build_classification_batches")
        return failed
    n_batches = len(batches)
    n_results = len([b for b in batch_results if b])
    if n_batches and n_results < n_batches:
        failed.append(f"classify_and_persist_batch ({n_batches - n_results} of {n_batches} batches)")
    return failed


def summarize_classification_run(**context):
    """Aggregate batch stats, log them, and record the run in paper_reuse_classification_runs."""
    ti = context["ti"]
    params = context["params"]
    edges = ti.xcom_pull(task_ids="fetch_unclassified_edges")
    batches = ti.xcom_pull(task_ids="build_classification_batches")
    batch_results = ti.xcom_pull(task_ids="classify_and_persist_batch") or []
    if isinstance(batch_results, dict):
        batch_results = [batch_results]
    batch_results = list(batch_results)
    failed_tasks = _failed_upstreams(edges, batches, batch_results)

    total = _merge_stats([b for b in batch_results if b])
    total["failed_tasks"] = failed_tasks
    model = _resolve_model(params.get("model"))
    scope = str(params.get("classification_scope", "citation_edges"))

    dag_run = context.get("dag_run")
    started_at = getattr(dag_run, "start_date", None) or datetime.now(timezone.utc)
    run_id = context.get("run_id", "unknown")

    logger.info(
        "=== Classification Run Summary ===\n"
        "  Model / prompt_version: %s / %s\n"
        "  Papers / pairs:         %d / %d\n"
        "  Classified:             %d   by label: %s\n"
        "  Errors:                 %d\n"
        "  No full text:           %d\n"
        "  Dry run rows:           %d\n"
        "  Guarded (not overwritten): %d\n"
        "  Text from cache / fetched: %d / %d\n"
        "  Hallucinated quotes:    %d   quote tiers: %s\n"
        "  Tokens prompt/completion/total: %d / %d / %d\n"
        "  Cost (where reported):  $%.4f over %d calls",
        model, PROMPT_VERSION, total["papers"], total["pairs"],
        total["classified"], json.dumps(total["by_category"]),
        total["errors"], total["no_full_text"], total["dry_run"], total["guarded"],
        total["text_from_cache"], total["text_fetched_on_demand"],
        total["hallucinated_quotes"], json.dumps(total["quote_tiers"]),
        total["usage"]["prompt_tokens"], total["usage"]["completion_tokens"], total["usage"]["total_tokens"],
        total["usage"]["cost_usd"], total["usage"]["cost_known_for"],
    )

    persisted_params = {k: params.get(k) for k in (
        "model", "reasoning_effort", "classification_scope", "source_filter", "max_edges_per_run",
        "batch_size", "dry_run", "reclassify_existing", "fetch_missing_fulltext",
        "temperature", "max_tokens", "max_input_chars", "max_retries", "min_credit_usd", "mix_publishers",
    )}
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO paper_reuse_classification_runs
                (run_id, dag_run_id, started_at, finished_at, model, prompt_version,
                 classification_scope, params, summary)
            VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s);
            """,
            (run_id, run_id, started_at, model, PROMPT_VERSION, scope,
             Json(persisted_params, dumps=lambda o: json.dumps(o, default=str)),
             Json(total, dumps=lambda o: json.dumps(o, default=str))),
        )
        conn.commit()

    if failed_tasks:
        raise AirflowFailException(
            "Run recorded, but upstream work did not finish: " + "; ".join(failed_tasks)
        )
    return total


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

def _build_dag_params() -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "model": DEFAULT_MODEL,
        "reasoning_effort": DEFAULT_REASONING_EFFORT,
        "max_edges_per_run": 100,
        "batch_size": 10,
        "classification_scope": "citation_edges",
        "max_retries": 3,
        "temperature": 0.0,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "max_input_chars": DEFAULT_MAX_INPUT_CHARS,
        "min_api_interval_seconds": 0.5,
        "fetch_missing_fulltext": True,
        "min_credit_usd": 5.0,
        "dry_run": False,
        "reclassify_existing": False,
        "mix_publishers": False,
    }
    if Param is not None:
        p["model"] = Param(DEFAULT_MODEL, type="string", title="Model",
                           description="OpenRouter model slug. Changing it makes every row eligible for reclassification.")
        p["reasoning_effort"] = Param(DEFAULT_REASONING_EFFORT, type="string", title="Reasoning effort",
                                      description="low | medium | high | max, or 'none' to omit the field.",
                                      enum=sorted(VALID_REASONING_EFFORTS) + ["none"])
        p["classification_scope"] = Param("citation_edges", type="string", title="Scope",
                                          description="citation_edges -> mode citing; primary_only -> mode direct; both.",
                                          enum=["citation_edges", "primary_only", "both"])
        p["batch_size"] = Param(10, type="integer", title="Papers per batch",
                                description="Papers (not pairs) per mapped task; a paper's pairs are never split.")
        p["max_edges_per_run"] = Param(100, type="integer", title="Max pairs per run",
                                       description="0 = no limit. Start small: each pair is one whole-paper LLM call.")
        p["fetch_missing_fulltext"] = Param(True, type="boolean", title="Fetch missing full text",
                                            description="Fetch on demand via paper-text-fetcher when no cached text exists.")
        p["mix_publishers"] = Param(False, type="boolean", title="Mix publishers",
                                    description="Off: pairs in citing-DOI order (a small run sees one publisher, "
                                                "e.g. 10.1002 = Wiley). On: round-robin across DOI prefixes for a "
                                                "mixed sample; reruns continue the same order.")
        p["min_credit_usd"] = Param(5.0, type="number", title="Minimum OpenRouter credit",
                                    description="Abort before classifying if the key's remaining credit is below this.")
        p["source_filter"] = Param(
            "all",
            title="Source repository",
            description=(
                "Same values as unified_datasets.source. Classification uses citation tables for "
                "DANDI, OpenNeuro, CRCNS and SPARC; Kaggle or PhysioNet select no edges."
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
    description="Whole-paper LLM classification of dataset reuse (REUSE / MENTION / NEITHER / PRIMARY) via OpenRouter",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["datasets", "papers", "classification", "llm"],
    params=_build_dag_params(),
)

schema_task = PythonOperator(
    task_id="create_classification_schema",
    python_callable=create_classification_schema,
    dag=dag,
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
    raise RuntimeError("Dynamic task mapping requires XComArg (airflow.models.xcom_arg).")

classify_task = (
    PythonOperator.partial(
        task_id="classify_and_persist_batch",
        python_callable=classify_and_persist_batch,
        pool=POOL_NAME,
        dag=dag,
    ).expand(op_kwargs=XComArg(build_batches_task))
)

summarize_task = PythonOperator(
    task_id="summarize_classification_run",
    python_callable=summarize_classification_run,
    trigger_rule="all_done",
    dag=dag,
)

schema_task >> fetch_task >> build_batches_task >> classify_task >> summarize_task
