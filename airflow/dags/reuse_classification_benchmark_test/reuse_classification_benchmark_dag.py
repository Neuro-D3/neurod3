"""
Reuse classification benchmark DAG (``reuse_classification_benchmark_test``).

A sanity test for the whole-paper reuse classifier. It runs the same fetcher,
prompt and model as ``paper_reuse_classification`` over a fixed answer key of
human-reviewed pairs (``benchmark_pairs.json``, built from find_reuse's
reuse_confirmation) and scores the labels against the reviewers' calls.

Pair sets (``pair_set`` param):
  smoke  20 fixed pairs (10 confirmed reuse, 10 rejected). Default locally.
  full   all 161 reviewed pairs. Default on staging, where
         REUSE_BENCHMARK_PAIR_SET=full is set in the compose file.

Metrics:
  reuse_recall      share of confirmed-reuse pairs labelled REUSE
  false_reuse_rate  share of rejected pairs labelled REUSE. These are hard
                    negatives: find_reuse's own classifier called every one of
                    them REUSE before a reviewer disagreed.
  text_coverage     share of pairs whose full text the fetcher returned
  reuse_type_agreement, per-pathway breakdowns, and every disagreement with
  the model's reasoning, for reading in the score task's log.

The run fails when a metric crosses its threshold param, so a red run means the
classifier (or the fetcher feeding it) has regressed. Results go only to
``reuse_benchmark_runs`` / ``reuse_benchmark_results``; production
classification tables are never touched.

It also records, at no LLM cost, how many answer-key pairs D3's own mapping
found (``mapping_coverage``): a pair the mapping never produces can never be
classified in production, whatever the classifier does.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
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
    from utils.database import get_db_connection
    from utils.llm_classify import get_openrouter_api_key, validate_openrouter_api_key
    from utils.classify_fulltext_reuse import (
        classify_paper_reuse,
        build_prompt,
        openrouter_credit_remaining,
        PROMPT_VERSION,
        DEFAULT_MODEL,
        DEFAULT_REASONING_EFFORT,
        VALID_REASONING_EFFORTS,
        DEFAULT_MAX_TOKENS,
        DEFAULT_MAX_INPUT_CHARS,
    )
    from utils.paper_fulltext import fetch_fulltext_detailed, load_cached_paper_text, TEXT_STATUS_FULL
except ImportError:  # pragma: no cover - direct import outside the dags folder
    from dags.utils.database import get_db_connection
    from dags.utils.llm_classify import get_openrouter_api_key, validate_openrouter_api_key
    from dags.utils.classify_fulltext_reuse import (
        classify_paper_reuse,
        build_prompt,
        openrouter_credit_remaining,
        PROMPT_VERSION,
        DEFAULT_MODEL,
        DEFAULT_REASONING_EFFORT,
        VALID_REASONING_EFFORTS,
        DEFAULT_MAX_TOKENS,
        DEFAULT_MAX_INPUT_CHARS,
    )
    from dags.utils.paper_fulltext import fetch_fulltext_detailed, load_cached_paper_text, TEXT_STATUS_FULL

logger = logging.getLogger(__name__)

DAG_ID = "reuse_classification_benchmark_test"
PAIRS_PATH = Path(__file__).resolve().parent / "benchmark_pairs.json"
# Shared with paper_reuse_classification so the two never hammer OpenRouter together.
POOL_NAME = "paper_reuse_classification_pool"

PAIR_SETS = ("smoke", "full")
DEFAULT_PAIR_SET = os.environ.get("REUSE_BENCHMARK_PAIR_SET", "smoke").strip().lower()
if DEFAULT_PAIR_SET not in PAIR_SETS:
    DEFAULT_PAIR_SET = "smoke"

# Provisional thresholds; tighten once a few full runs have set a baseline.
DEFAULT_MIN_REUSE_RECALL = 0.8
DEFAULT_MAX_FALSE_REUSE_RATE = 0.5
DEFAULT_MIN_TEXT_COVERAGE = 0.8

STATUS_CLASSIFIED = "classified"
STATUS_ERROR = "error"
STATUS_NO_FULL_TEXT = "no_full_text"
STATUS_DRY_RUN = "dry_run"


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in test_reuse_classification_benchmark.py)
# ---------------------------------------------------------------------------

def load_answer_key(path: Path = PAIRS_PATH) -> Dict[str, Any]:
    raw = path.read_bytes()
    data = json.loads(raw.decode("utf-8"))
    data["sha256"] = hashlib.sha256(raw).hexdigest()
    return data


def select_pairs(pairs: List[Dict[str, Any]], pair_set: str, max_pairs: int = 0) -> List[Dict[str, Any]]:
    """The pairs a run classifies: the smoke subset or all, optionally capped."""
    pair_set = (pair_set or "").strip().lower()
    if pair_set not in PAIR_SETS:
        raise AirflowFailException(f"pair_set={pair_set!r} is not one of {list(PAIR_SETS)}")
    chosen = [p for p in pairs if p.get("smoke")] if pair_set == "smoke" else list(pairs)
    if max_pairs and max_pairs > 0:
        chosen = chosen[:max_pairs]
    return chosen


def group_by_paper(pairs: List[Dict[str, Any]], papers_per_batch: int) -> List[List[List[Dict[str, Any]]]]:
    """
    Batches of paper groups. A paper's pairs stay together so its text is
    fetched once and the provider's prompt cache covers the repeat calls.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for p in pairs:
        groups.setdefault(p["fetched_doi"], []).append(p)
    papers_per_batch = max(1, int(papers_per_batch))
    ordered = list(groups.values())
    return [ordered[i:i + papers_per_batch] for i in range(0, len(ordered), papers_per_batch)]


def resolve_reasoning_effort(raw: Any) -> Optional[str]:
    s = (str(raw) if raw is not None else "").strip().lower()
    if not s or s == "none":
        return None
    if s not in VALID_REASONING_EFFORTS:
        raise AirflowFailException(f"reasoning_effort={raw!r} is not one of {sorted(VALID_REASONING_EFFORTS)} or 'none'")
    return s


def _ratio(num: int, den: int) -> Optional[float]:
    return round(num / den, 4) if den else None


def score_results(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Metrics over one run's result rows.

    Only rows with a real label count toward recall and false-reuse rate; rows
    without full text or with an API error are counted separately so a fetcher
    outage reads as low coverage, not as a classifier that stopped finding reuse.
    """
    def _block(rs: List[Dict[str, Any]]) -> Dict[str, Any]:
        labelled = [r for r in rs if r.get("status") == STATUS_CLASSIFIED]
        pos = [r for r in labelled if r["expected"] == "REUSE"]
        neg = [r for r in labelled if r["expected"] == "NOT_REUSE"]
        tp = sum(r.get("label") == "REUSE" for r in pos)
        fp = sum(r.get("label") == "REUSE" for r in neg)
        correct = tp + (len(neg) - fp)
        return {
            "pairs": len(rs),
            "labelled": len(labelled),
            "no_full_text": sum(r.get("status") == STATUS_NO_FULL_TEXT for r in rs),
            "errors": sum(r.get("status") == STATUS_ERROR for r in rs),
            "expected_reuse": len(pos),
            "expected_not_reuse": len(neg),
            "true_reuse": tp,
            "false_reuse": fp,
            "reuse_recall": _ratio(tp, len(pos)),
            "false_reuse_rate": _ratio(fp, len(neg)),
            "accuracy": _ratio(correct, len(labelled)),
        }

    overall = _block(rows)
    overall["text_coverage"] = _ratio(
        sum(r.get("status") != STATUS_NO_FULL_TEXT for r in rows), len(rows))

    hits = [r for r in rows if r.get("status") == STATUS_CLASSIFIED
            and r["expected"] == "REUSE" and r.get("label") == "REUSE"]
    typed = [r for r in hits if r.get("expected_reuse_types")]
    overall["reuse_type_agreement"] = _ratio(
        sum(r.get("reuse_type") in (r.get("expected_reuse_types") or []) for r in typed), len(typed))

    labels: Dict[str, int] = {}
    for r in rows:
        k = r.get("label") or r.get("status") or "unknown"
        labels[k] = labels.get(k, 0) + 1
    overall["labels"] = dict(sorted(labels.items()))

    by_pathway = {}
    for pathway in sorted({r.get("pathway") or "unknown" for r in rows}):
        by_pathway[pathway] = _block([r for r in rows if (r.get("pathway") or "unknown") == pathway])
    overall["by_pathway"] = by_pathway

    overall["disagreements"] = [
        {
            "pair_id": r["pair_id"],
            "pathway": r.get("pathway"),
            "human_call": r.get("human_call"),
            "label": r.get("label"),
            "confidence": r.get("confidence"),
            "reasoning": (r.get("reasoning") or "")[:400],
        }
        for r in rows
        if r.get("status") == STATUS_CLASSIFIED
        and (r.get("label") == "REUSE") != (r["expected"] == "REUSE")
    ]
    return overall


def check_thresholds(metrics: Dict[str, Any], min_recall: float, max_false_reuse: float,
                     min_coverage: float) -> List[str]:
    """Human-readable failures; empty means the run passed."""
    failures: List[str] = []
    cov = metrics.get("text_coverage")
    if cov is None or cov < min_coverage:
        failures.append(f"text_coverage {cov} < {min_coverage} (fetcher problem, results inconclusive)")
    rec = metrics.get("reuse_recall")
    if rec is None:
        failures.append("reuse_recall undefined: no confirmed-reuse pair got a label")
    elif rec < min_recall:
        failures.append(f"reuse_recall {rec} < {min_recall}")
    fr = metrics.get("false_reuse_rate")
    if fr is None:
        failures.append("false_reuse_rate undefined: no rejected pair got a label")
    elif fr > max_false_reuse:
        failures.append(f"false_reuse_rate {fr} > {max_false_reuse}")
    if metrics.get("errors"):
        failures.append(f"{metrics['errors']} pairs ended in a classifier error")
    return failures


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS reuse_benchmark_runs (
    id                SERIAL PRIMARY KEY,
    run_id            TEXT UNIQUE NOT NULL,
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    pair_set          TEXT,
    model             TEXT,
    reasoning_effort  TEXT,
    prompt_version    INTEGER,
    answer_key_sha256 TEXT,
    upstream_commit   TEXT,
    dry_run           BOOLEAN,
    passed            BOOLEAN,
    failures          JSONB,
    thresholds        JSONB,
    metrics           JSONB,
    mapping_coverage  JSONB,
    usage             JSONB
);

CREATE TABLE IF NOT EXISTS reuse_benchmark_results (
    id                        SERIAL PRIMARY KEY,
    run_id                    TEXT NOT NULL,
    pair_id                   TEXT NOT NULL,
    paper_doi                 TEXT,
    fetched_doi               TEXT,
    dataset_source            TEXT,
    dataset_id                TEXT,
    primary_paper_doi         TEXT,
    pathway                   TEXT,
    mode                      TEXT,
    human_call                TEXT,
    expected                  TEXT,
    expected_reuse_types      JSONB,
    status                    TEXT,
    label                     TEXT,
    confidence                INTEGER,
    reuse_type                TEXT,
    reuse_type_other          TEXT,
    same_lab                  BOOLEAN,
    reasoning                 TEXT,
    evidence_quotes           JSONB,
    hallucinated_quote_count  INTEGER,
    text_source               TEXT,
    text_chars                INTEGER,
    error_kind                TEXT,
    usage                     JSONB,
    classified_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (run_id, pair_id)
);
CREATE INDEX IF NOT EXISTS idx_reuse_benchmark_results_run ON reuse_benchmark_results(run_id);
"""


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

def prepare_benchmark(**context) -> Dict[str, Any]:
    """Create the tables, check the OpenRouter key and credit, pick the pairs."""
    params = context["params"]
    dry_run = bool(params.get("dry_run", False))
    key = load_answer_key()
    pairs = select_pairs(key["pairs"], params.get("pair_set", DEFAULT_PAIR_SET), int(params.get("max_pairs", 0) or 0))

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(SCHEMA_SQL)
        conn.commit()

    if not dry_run:
        api_key = get_openrouter_api_key()
        validate_openrouter_api_key(api_key)
        model = (params.get("model") or DEFAULT_MODEL).strip()
        min_credit = float(params.get("min_credit_usd", 0) or 0)
        remaining = openrouter_credit_remaining(model)
        if remaining is not None and remaining < min_credit:
            raise AirflowFailException(
                f"OpenRouter credit ${remaining:.2f} is below min_credit_usd ${min_credit:.2f}")
        logger.info("OpenRouter credit remaining: %s", remaining)

    logger.info("Benchmark %s: %d pairs (answer key %s, upstream %s)",
                params.get("pair_set"), len(pairs), key["sha256"][:12], key.get("upstream_commit"))
    return {
        "pairs": pairs,
        "answer_key_sha256": key["sha256"],
        "upstream_commit": key.get("upstream_commit"),
    }


def build_benchmark_batches(**context) -> List[Dict[str, Any]]:
    prepared = context["ti"].xcom_pull(task_ids="prepare_benchmark") or {}
    batches = group_by_paper(prepared.get("pairs") or [], int(context["params"].get("papers_per_batch", 5)))
    logger.info("Built %d batches", len(batches))
    return [{"batch_index": i, "paper_groups": b} for i, b in enumerate(batches)]


_RESULT_UPSERT = """
INSERT INTO reuse_benchmark_results (
    run_id, pair_id, paper_doi, fetched_doi, dataset_source, dataset_id, primary_paper_doi,
    pathway, mode, human_call, expected, expected_reuse_types,
    status, label, confidence, reuse_type, reuse_type_other, same_lab, reasoning,
    evidence_quotes, hallucinated_quote_count, text_source, text_chars, error_kind, usage, classified_at
) VALUES (
    %(run_id)s, %(pair_id)s, %(paper_doi)s, %(fetched_doi)s, %(dataset_source)s, %(dataset_id)s, %(primary_paper_doi)s,
    %(pathway)s, %(mode)s, %(human_call)s, %(expected)s, %(expected_reuse_types)s,
    %(status)s, %(label)s, %(confidence)s, %(reuse_type)s, %(reuse_type_other)s, %(same_lab)s, %(reasoning)s,
    %(evidence_quotes)s, %(hallucinated_quote_count)s, %(text_source)s, %(text_chars)s, %(error_kind)s, %(usage)s, NOW()
)
ON CONFLICT (run_id, pair_id) DO UPDATE SET
    status = EXCLUDED.status, label = EXCLUDED.label, confidence = EXCLUDED.confidence,
    reuse_type = EXCLUDED.reuse_type, reuse_type_other = EXCLUDED.reuse_type_other,
    same_lab = EXCLUDED.same_lab, reasoning = EXCLUDED.reasoning,
    evidence_quotes = EXCLUDED.evidence_quotes, hallucinated_quote_count = EXCLUDED.hallucinated_quote_count,
    text_source = EXCLUDED.text_source, text_chars = EXCLUDED.text_chars,
    error_kind = EXCLUDED.error_kind, usage = EXCLUDED.usage, classified_at = NOW();
"""


# unified_datasets.source label -> per-archive dataset table
_DATASET_TABLES = {"DANDI": "dandi_dataset", "OpenNeuro": "openneuro_dataset",
                   "CRCNS": "crcns_dataset", "SPARC": "sparc_dataset"}


def _dataset_description(cursor, source: str, dataset_id: str) -> str:
    """Same description the production DAG hands the model, when this DB has the dataset."""
    queries = [(
        "SELECT COALESCE(full_description, description) FROM unified_datasets "
        "WHERE source = %s AND dataset_id = %s LIMIT 1;", (source, dataset_id))]
    table = _DATASET_TABLES.get(source)
    if table:
        queries.append((f"SELECT description FROM {table} WHERE dataset_id = %s LIMIT 1;", (dataset_id,)))
    for sql, args in queries:
        try:
            cursor.execute("SAVEPOINT ds_lookup;")
            cursor.execute(sql, args)
            row = cursor.fetchone()
            cursor.execute("RELEASE SAVEPOINT ds_lookup;")
            if row and row[0]:
                return row[0]
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT ds_lookup;")
    return ""


def _result_row(run_id: str, pair: Dict[str, Any], **fields: Any) -> Dict[str, Any]:
    row = {
        "run_id": run_id,
        "pair_id": pair["pair_id"],
        "paper_doi": pair["paper_doi"],
        "fetched_doi": pair["fetched_doi"],
        "dataset_source": pair.get("dataset_source", "DANDI"),
        "dataset_id": pair["dataset_id"],
        "primary_paper_doi": pair.get("primary_paper_doi") or None,
        "pathway": pair.get("pathway"),
        "mode": pair.get("mode"),
        "human_call": pair.get("human_call"),
        "expected": pair["expected"],
        "expected_reuse_types": Json(pair.get("find_reuse_reuse_types") or []),
        "status": None, "label": None, "confidence": None, "reuse_type": None,
        "reuse_type_other": None, "same_lab": None, "reasoning": None,
        "evidence_quotes": Json([]), "hallucinated_quote_count": 0,
        "text_source": None, "text_chars": None, "error_kind": None, "usage": None,
    }
    row.update(fields)
    return row


def classify_benchmark_batch(*, batch_index: int, paper_groups: List[List[Dict[str, Any]]], **context) -> Dict[str, Any]:
    params = context["params"]
    run_id = context.get("run_id", "unknown")
    dry_run = bool(params.get("dry_run", False))
    model = (params.get("model") or DEFAULT_MODEL).strip()
    reasoning_effort = resolve_reasoning_effort(params.get("reasoning_effort", DEFAULT_REASONING_EFFORT))
    interval = float(params.get("min_api_interval_seconds", 0.5))
    api_key = None if dry_run else get_openrouter_api_key()

    done = 0
    fatal: Optional[str] = None
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for group in paper_groups:
            if fatal:
                break
            doi = group[0]["fetched_doi"]
            # Same order as paper_reuse_classification: the mapping DAGs' cache,
            # then the fetcher. The benchmark should read what production reads.
            text = load_cached_paper_text(cursor, doi)
            fetched: Dict[str, Any] = {"source": "cache", "status": TEXT_STATUS_FULL}
            if not text:
                fetched = fetch_fulltext_detailed(doi)
                text = fetched.get("text") if fetched.get("status") == TEXT_STATUS_FULL else None
            if not text:
                logger.info("No full text for %s: %s (%s)", doi, fetched.get("status"), fetched.get("reason"))

            for pair in group:
                if not text:
                    row = _result_row(run_id, pair, status=STATUS_NO_FULL_TEXT, text_source=fetched.get("source"),
                                      error_kind=STATUS_NO_FULL_TEXT, reasoning=fetched.get("reason"))
                elif dry_run:
                    prompt = build_prompt(text, dataset_id=pair["dataset_id"], dataset_name=pair.get("dataset_name", ""),
                                          dataset_description=_dataset_description(cursor, pair.get("dataset_source", "DANDI"), pair["dataset_id"]),
                                          primary_paper_doi=pair.get("primary_paper_doi") or "", mode=pair["mode"])
                    logger.info("[DRY RUN] %s mode=%s prompt_chars=%d", pair["pair_id"], pair["mode"], len(prompt))
                    row = _result_row(run_id, pair, status=STATUS_DRY_RUN, text_source=fetched.get("source"),
                                      text_chars=len(text))
                else:
                    result = classify_paper_reuse(
                        text,
                        dataset_id=pair["dataset_id"],
                        dataset_name=pair.get("dataset_name", ""),
                        dataset_description=_dataset_description(cursor, pair.get("dataset_source", "DANDI"), pair["dataset_id"]),
                        primary_paper_doi=pair.get("primary_paper_doi") or "",
                        paper_doi=doi,
                        api_key=api_key,
                        model=model,
                        max_input_chars=int(params.get("max_input_chars", DEFAULT_MAX_INPUT_CHARS)),
                        max_tokens=int(params.get("max_tokens", DEFAULT_MAX_TOKENS)),
                        temperature=0.0,
                        max_retries=int(params.get("max_retries", 3)),
                        mode=pair["mode"],
                        reasoning_effort=reasoning_effort,
                    )
                    label = result.get("classification")
                    is_error = label == "ERROR"
                    row = _result_row(
                        run_id, pair,
                        status=STATUS_ERROR if is_error else STATUS_CLASSIFIED,
                        label=None if is_error else label,
                        confidence=result.get("confidence"),
                        reuse_type=result.get("reuse_type"),
                        reuse_type_other=result.get("reuse_type_other"),
                        same_lab=result.get("same_lab"),
                        reasoning=result.get("reasoning") or result.get("error"),
                        evidence_quotes=Json(result.get("evidence_quotes") or []),
                        hallucinated_quote_count=int(result.get("hallucinated_quote_count") or 0),
                        text_source=fetched.get("source"),
                        text_chars=len(text),
                        error_kind=result.get("error_kind"),
                        usage=Json(result["usage"]) if isinstance(result.get("usage"), dict) else None,
                    )
                    if is_error and result.get("fatal"):
                        fatal = f"{result.get('error_kind')}: {result.get('error')}"
                    if interval > 0:
                        time.sleep(interval)
                cursor.execute(_RESULT_UPSERT, row)
                done += 1
                if fatal:
                    break
        conn.commit()

    if fatal:
        raise AirflowFailException(f"Fatal API error in batch {batch_index}: {fatal}")
    return {"batch_index": batch_index, "pairs": done}


def _mapping_coverage(cursor, pairs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """How many answer-key pairs D3's mapping produced. DB only, no API calls."""
    def _exists(sql: str, args: Tuple) -> bool:
        cursor.execute("SAVEPOINT cov;")
        try:
            cursor.execute(sql, args)
            ok = bool(cursor.fetchone())
            cursor.execute("RELEASE SAVEPOINT cov;")
            return ok
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT cov;")
            return False

    out = {"pairs": len(pairs), "dataset_has_primary_paper": 0, "citation_edge_exists": 0,
           "indirect_pairs": 0, "indirect_edge_exists": 0}
    for p in pairs:
        ds = p["dataset_id"]
        if _exists("SELECT 1 FROM dandi_paper_map WHERE dandi_id = %s LIMIT 1;", (ds,)):
            out["dataset_has_primary_paper"] += 1
        edge = _exists(
            "SELECT 1 FROM dandi_paper_citations WHERE dandi_id = %s AND lower(citing_paper_doi) IN (%s, %s) LIMIT 1;",
            (ds, p["paper_doi"], p["fetched_doi"]))
        out["citation_edge_exists"] += int(edge)
        if p.get("pathway") == "indirect":
            out["indirect_pairs"] += 1
            out["indirect_edge_exists"] += int(edge)
    return out


def score_benchmark_run(**context) -> Dict[str, Any]:
    """Score the run from its stored rows, record it, and fail on a regression."""
    params = context["params"]
    run_id = context.get("run_id", "unknown")
    dry_run = bool(params.get("dry_run", False))
    prepared = context["ti"].xcom_pull(task_ids="prepare_benchmark") or {}
    pairs = prepared.get("pairs") or []

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT pair_id, pathway, human_call, expected, expected_reuse_types, status, label, "
            "confidence, reuse_type, reasoning, usage FROM reuse_benchmark_results WHERE run_id = %s;",
            (run_id,),
        )
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, r, strict=True)) for r in cursor.fetchall()]
        coverage = _mapping_coverage(cursor, pairs)

        metrics = score_results(rows)
        metrics["pairs_selected"] = len(pairs)
        metrics["pairs_missing"] = len(pairs) - len(rows)
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        for r in rows:
            u = r.get("usage") or {}
            for k in usage:
                usage[k] += int(u.get(k) or 0)

        thresholds = {
            "min_reuse_recall": float(params.get("min_reuse_recall", DEFAULT_MIN_REUSE_RECALL)),
            "max_false_reuse_rate": float(params.get("max_false_reuse_rate", DEFAULT_MAX_FALSE_REUSE_RATE)),
            "min_text_coverage": float(params.get("min_text_coverage", DEFAULT_MIN_TEXT_COVERAGE)),
        }
        if dry_run:
            failures = []
            if metrics["text_coverage"] is None or metrics["text_coverage"] < thresholds["min_text_coverage"]:
                failures.append(f"text_coverage {metrics['text_coverage']} < {thresholds['min_text_coverage']}")
        else:
            failures = check_thresholds(metrics, thresholds["min_reuse_recall"],
                                        thresholds["max_false_reuse_rate"], thresholds["min_text_coverage"])
        if metrics["pairs_missing"]:
            failures.append(f"{metrics['pairs_missing']} selected pairs have no result (a batch failed)")

        dag_run = context.get("dag_run")
        started_at = getattr(dag_run, "start_date", None) or datetime.now(timezone.utc)
        cursor.execute(
            """
            INSERT INTO reuse_benchmark_runs
                (run_id, started_at, finished_at, pair_set, model, reasoning_effort, prompt_version,
                 answer_key_sha256, upstream_commit, dry_run, passed, failures, thresholds, metrics,
                 mapping_coverage, usage)
            VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                finished_at = EXCLUDED.finished_at, passed = EXCLUDED.passed, failures = EXCLUDED.failures,
                metrics = EXCLUDED.metrics, mapping_coverage = EXCLUDED.mapping_coverage, usage = EXCLUDED.usage;
            """,
            (run_id, started_at, params.get("pair_set"), (params.get("model") or DEFAULT_MODEL).strip(),
             params.get("reasoning_effort"), PROMPT_VERSION, prepared.get("answer_key_sha256"),
             prepared.get("upstream_commit"), dry_run, not failures, Json(failures), Json(thresholds),
             Json(metrics, dumps=lambda o: json.dumps(o, default=str)), Json(coverage), Json(usage)),
        )
        conn.commit()

    report = {k: metrics[k] for k in ("pairs", "labelled", "no_full_text", "errors", "text_coverage",
                                      "reuse_recall", "false_reuse_rate", "accuracy", "reuse_type_agreement",
                                      "labels")}
    logger.info("=== Reuse benchmark (%s, %s, prompt v%s) ===\n%s\nby pathway: %s\nmapping coverage: %s\ntokens: %s",
                params.get("pair_set"), "DRY RUN" if dry_run else "live", PROMPT_VERSION,
                json.dumps(report, indent=2), json.dumps(metrics["by_pathway"], indent=2),
                json.dumps(coverage), json.dumps(usage))
    for d in metrics["disagreements"]:
        logger.info("DISAGREE %s [%s] human=%s model=%s (conf %s): %s", d["pair_id"], d["pathway"],
                    d["human_call"], d["label"], d["confidence"], d["reasoning"])

    if failures:
        raise AirflowFailException("Reuse benchmark FAILED: " + "; ".join(failures))
    logger.info("Reuse benchmark PASSED")
    return report


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

def _build_params() -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "pair_set": DEFAULT_PAIR_SET,
        "max_pairs": 0,
        "dry_run": False,
        "model": DEFAULT_MODEL,
        "reasoning_effort": DEFAULT_REASONING_EFFORT,
        "papers_per_batch": 5,
        "min_reuse_recall": DEFAULT_MIN_REUSE_RECALL,
        "max_false_reuse_rate": DEFAULT_MAX_FALSE_REUSE_RATE,
        "min_text_coverage": DEFAULT_MIN_TEXT_COVERAGE,
        "min_credit_usd": 5.0,
        "max_retries": 3,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "max_input_chars": DEFAULT_MAX_INPUT_CHARS,
        "min_api_interval_seconds": 0.5,
    }
    if Param is not None:
        p["pair_set"] = Param(DEFAULT_PAIR_SET, type="string", enum=list(PAIR_SETS), title="Pair set",
                              description="smoke = 20 fixed pairs; full = all 161 reviewed pairs. "
                                          "Default comes from REUSE_BENCHMARK_PAIR_SET (full on staging).")
        p["max_pairs"] = Param(0, type="integer", title="Max pairs", description="0 = the whole pair set.")
        p["dry_run"] = Param(False, type="boolean", title="Dry run",
                             description="Fetch text and build prompts, but make no LLM calls. Checks the fetcher.")
        p["reasoning_effort"] = Param(DEFAULT_REASONING_EFFORT, type="string", title="Reasoning effort",
                                      enum=sorted(VALID_REASONING_EFFORTS) + ["none"])
        p["min_reuse_recall"] = Param(DEFAULT_MIN_REUSE_RECALL, type="number", title="Min reuse recall",
                                      description="Fail below this share of confirmed-reuse pairs labelled REUSE.")
        p["max_false_reuse_rate"] = Param(DEFAULT_MAX_FALSE_REUSE_RATE, type="number", title="Max false-reuse rate",
                                          description="Fail above this share of rejected pairs labelled REUSE.")
        p["min_text_coverage"] = Param(DEFAULT_MIN_TEXT_COVERAGE, type="number", title="Min text coverage",
                                       description="Fail (inconclusive) when fewer pairs than this got full text.")
    return p


dag = DAG(
    DAG_ID,
    default_args={
        "owner": "neurod3",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="Sanity test: score the reuse classifier against find_reuse's human-reviewed pairs",
    schedule=None,
    catchup=False,
    # Manual/deploy-triggered only (no schedule), so unpaused is safe and lets a
    # CLI trigger from the staging deploy run without a separate unpause step.
    is_paused_upon_creation=False,
    tags=["papers", "classification", "llm", "benchmark", "test"],
    params=_build_params(),
)

prepare_task = PythonOperator(task_id="prepare_benchmark", python_callable=prepare_benchmark, dag=dag)
batches_task = PythonOperator(task_id="build_benchmark_batches", python_callable=build_benchmark_batches, dag=dag)
classify_task = PythonOperator.partial(
    task_id="classify_benchmark_batch",
    python_callable=classify_benchmark_batch,
    pool=POOL_NAME,
    dag=dag,
).expand(op_kwargs=batches_task.output)
score_task = PythonOperator(
    task_id="score_benchmark_run",
    python_callable=score_benchmark_run,
    trigger_rule="all_done",  # always record the run, even after a failed batch
    dag=dag,
)

prepare_task >> batches_task >> classify_task >> score_task
