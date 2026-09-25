"""
Stack integration test DAG (``stack_integration_test``).

Proves the whole pipeline is wired together after a deploy, on one known
dataset per archive:

    preflight ─▶ ingest ─▶ verify ─▶ map papers (+ small citing-paper backfill)
              ─▶ verify ─▶ classify a few pairs (real LLM calls) ─▶ verify
              ─▶ API check ─▶ site check ─▶ report

Each stage triggers the real, deployed DAG (``<archive>_ingestion``,
``<archive>_paper_mapping``, ``paper_reuse_classification``) on just the test
dataset via its ``dataset_ids`` param and waits for it, so scheduling, pools,
the image and task wiring are exercised, not only the Python. The four
archives run side by side; a failure stops that archive's chain and the others
carry on.

Every step writes a row to ``integration_test_steps`` (stage, archive, status,
details, duration, error and traceback, and which triggered DAG run to look
at). ``report`` runs last whatever happened, logs a one-screen report, records
the run in ``integration_test_runs`` and fails the run naming every failed
step.

Staging runs it after every deploy (deploy-staging.yml). Locally, trigger it
from the UI. It re-classifies the test pairs on every run (a handful of
whole-paper LLM calls) so an expired key or a broken model is caught.
"""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from psycopg2.extras import Json

try:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except Exception:  # pragma: no cover
    from airflow.operators.python import PythonOperator  # type: ignore
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

try:
    from airflow.sdk import Param  # Airflow 3+
except Exception:  # pragma: no cover
    from airflow.models.param import Param  # type: ignore

try:
    from utils.database import apply_schema_ddl, get_db_connection
    from utils.classify_fulltext_reuse import PROMPT_VERSION, DEFAULT_MODEL, openrouter_credit_remaining
    from utils.llm_classify import get_openrouter_api_key, validate_openrouter_api_key
    from utils.openalex_budget import check_openalex_budget
    from utils.paper_fulltext import get_paper_fetcher
except ImportError:  # pragma: no cover
    from dags.utils.database import apply_schema_ddl, get_db_connection
    from dags.utils.classify_fulltext_reuse import PROMPT_VERSION, DEFAULT_MODEL, openrouter_credit_remaining
    from dags.utils.llm_classify import get_openrouter_api_key, validate_openrouter_api_key
    from dags.utils.openalex_budget import check_openalex_budget
    from dags.utils.paper_fulltext import get_paper_fetcher

logger = logging.getLogger(__name__)

DAG_ID = "stack_integration_test"

# One test dataset per archive. ingest_id is what the ingestion DAG matches on
# (CRCNS listings carry the DOI before the short code is resolved).
# DANDI 000402 (MICrONS) is a known REUSE (find_reuse's reviewers and our own
# classifier); the others have a mapped primary paper whose citing papers have
# full text. CRCNS alm-3 is still keyed by its DOI on staging, so its run also
# exercises the DOI -> code re-key.
ARCHIVES: Dict[str, Dict[str, str]] = {
    # reuse_citing_doi: a citing paper known to reuse the dataset. Its pair is
    # re-classified first every run and must come back REUSE. 000402's is the
    # vascular basement membrane paper that analyzed the MICrONS EM volume
    # (REUSE 10/10 here; the dataset is in find_reuse's reviewed reuse set).
    "dandi": {"label": "DANDI", "dataset_id": "000402", "ingest_id": "000402",
              "reuse_citing_doi": "10.1186/s12987-023-00425-4"},
    "openneuro": {"label": "OpenNeuro", "dataset_id": "ds004213", "ingest_id": "ds004213", "reuse_citing_doi": ""},
    "crcns": {"label": "CRCNS", "dataset_id": "alm-3", "ingest_id": "10.6080/k0rb72jw", "reuse_citing_doi": ""},
    "sparc": {"label": "SPARC", "dataset_id": "308", "ingest_id": "308", "reuse_citing_doi": ""},
}

# Per-archive stages in order; used to lay out the report.
STAGES = ["ingest", "verify_ingest", "map", "verify_map", "classify", "verify_classify", "check_api"]
GLOBAL_STEPS = ["preflight_database", "preflight_openrouter", "preflight_openalex", "preflight_fetcher",
                "preflight_api", "check_site"]

# Triggered DAG for each trigger stage.
TRIGGERED_DAG = {"ingest": "{key}_ingestion", "map": "{key}_paper_mapping", "classify": "paper_reuse_classification"}

STATUS_PASS, STATUS_FAIL, STATUS_WARN = "pass", "fail", "warn"


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in test_stack_integration.py)
# ---------------------------------------------------------------------------

def triggered_run_id(run_id: str, key: str, stage: str) -> str:
    """Run id given to a triggered DAG run, so the report can point at it."""
    return f"it__{run_id}__{key}__{stage}"


def build_report(steps: List[Dict[str, Any]], archives: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    """
    Lay step rows out as a matrix, mark steps the run never reached, and
    collect the failures. A missing row after a failed step reads as
    "not reached"; a missing row with nothing failed before it (the run was
    killed, a callback could not write) reads as "missing".
    """
    by_key = {(s["archive"] or "", s["step"]): s for s in steps}
    matrix: Dict[str, Dict[str, str]] = {}
    failures: List[str] = []
    warnings: List[str] = []

    for step in GLOBAL_STEPS:
        s = by_key.get(("", step))
        status = s["status"] if s else "missing"
        matrix.setdefault("(global)", {})[step] = status
        if status == STATUS_FAIL:
            failures.append(f"{step}: {(s.get('error') or '').splitlines()[0][:200] if s else ''}")
        elif status == STATUS_WARN:
            warnings.append(f"{step}: {(s.get('details') or {}).get('warning', '')}")

    preflight_failed = any(matrix["(global)"].get(g) == STATUS_FAIL for g in GLOBAL_STEPS if g.startswith("preflight"))
    for key, cfg in archives.items():
        label = cfg["label"]
        row: Dict[str, str] = {}
        broken = preflight_failed
        for stage in STAGES:
            s = by_key.get((label, stage))
            if s:
                status = s["status"]
            else:
                status = "not reached" if broken else "missing"
            row[stage] = status
            if status == STATUS_FAIL:
                broken = True
                err = (s.get("error") or "").strip().splitlines()
                hint = f" (see {s['log_hint']})" if s.get("log_hint") else ""
                failures.append(f"{label} {stage}: {err[0][:200] if err else 'failed'}{hint}")
            elif status == "missing":
                broken = True
                failures.append(f"{label} {stage}: no result recorded (task killed or never ran)")
            elif status == STATUS_WARN:
                warnings.append(f"{label} {stage}: {(s.get('details') or {}).get('warning', '')}")
        matrix[label] = row

    return {"matrix": matrix, "failures": failures, "warnings": warnings, "passed": not failures}


def format_report(report: Dict[str, Any]) -> str:
    """A fixed-width table, one line per archive, for the task log."""
    cols = ["ingest", "verify_ingest", "map", "verify_map", "classify", "verify_classify", "check_api"]
    short = {"ingest": "ingest", "verify_ingest": "v_ing", "map": "map", "verify_map": "v_map",
             "classify": "classify", "verify_classify": "v_cls", "check_api": "api"}
    lines = ["archive    " + " ".join(f"{short[c]:<11}" for c in cols)]
    for label, row in report["matrix"].items():
        if label == "(global)":
            continue
        lines.append(f"{label:<10} " + " ".join(f"{row.get(c, ''):<11}" for c in cols))
    glob = report["matrix"].get("(global)", {})
    lines.append("global: " + ", ".join(f"{k}={v}" for k, v in glob.items()))
    lines.append("RESULT: " + ("PASSED" if report["passed"] else f"FAILED ({len(report['failures'])} failing step(s))"))
    for f in report["failures"]:
        lines.append("  FAIL " + f)
    for w in report["warnings"]:
        lines.append("  warn " + w)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Step recording
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS integration_test_runs (
    id           SERIAL PRIMARY KEY,
    run_id       TEXT UNIQUE NOT NULL,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    passed       BOOLEAN,
    failures     JSONB,
    warnings     JSONB,
    matrix       JSONB,
    config       JSONB
);

CREATE TABLE IF NOT EXISTS integration_test_steps (
    id           SERIAL PRIMARY KEY,
    run_id       TEXT NOT NULL,
    step         TEXT NOT NULL,
    archive      TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    duration_s   DOUBLE PRECISION,
    details      JSONB,
    error        TEXT,
    traceback    TEXT,
    log_hint     TEXT,
    UNIQUE (run_id, step, archive)
);
CREATE INDEX IF NOT EXISTS idx_integration_test_steps_run ON integration_test_steps(run_id);
"""


# Every writer runs SCHEMA_SQL through apply_schema_ddl, which skips it once the
# table and index exist: a plain CREATE INDEX IF NOT EXISTS takes a SHARE lock
# even when the index exists, and four archives recording at once deadlocked
# on it (each holding SHARE, each waiting to INSERT).
def _record(run_id: str, step: str, archive: str, status: str, *, details: Optional[Dict[str, Any]] = None,
            error: Optional[str] = None, tb: Optional[str] = None, started: Optional[float] = None,
            log_hint: Optional[str] = None) -> None:
    now = datetime.now(timezone.utc)
    started_at = datetime.fromtimestamp(started, timezone.utc) if started else now
    duration = round(time.time() - started, 2) if started else None
    with get_db_connection() as conn:
        cur = conn.cursor()
        apply_schema_ddl(cur, SCHEMA_SQL)
        cur.execute(
            """
            INSERT INTO integration_test_steps
                (run_id, step, archive, status, started_at, finished_at, duration_s, details, error, traceback, log_hint)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, step, archive) DO UPDATE SET
                status = EXCLUDED.status, started_at = EXCLUDED.started_at, finished_at = EXCLUDED.finished_at,
                duration_s = EXCLUDED.duration_s, details = EXCLUDED.details, error = EXCLUDED.error,
                traceback = EXCLUDED.traceback, log_hint = EXCLUDED.log_hint;
            """,
            (run_id, step, archive or "", status, started_at, now, duration,
             Json(details or {}, dumps=lambda o: json.dumps(o, default=str)), error, tb, log_hint),
        )
        conn.commit()


class _Step:
    """Collects details and a warning while a check runs; see `step()`."""

    def __init__(self) -> None:
        self.details: Dict[str, Any] = {}
        self.warning: Optional[str] = None

    def warn(self, message: str) -> None:
        self.warning = message if not self.warning else f"{self.warning}; {message}"


@contextmanager
def step(context: Dict[str, Any], name: str, archive: str = "", log_hint: Optional[str] = None) -> Iterator[_Step]:
    """
    Run one check and record it. Any exception is recorded with its traceback
    and re-raised as AirflowFailException (a check failing is not worth a
    retry); a check can also call `s.warn()` to pass with a warning.
    """
    run_id = context.get("run_id", "unknown")
    s = _Step()
    t0 = time.time()
    logger.info("── %s %s", archive or "global", name)
    try:
        yield s
    except Exception as e:
        _record(run_id, name, archive, STATUS_FAIL, details=s.details, error=f"{type(e).__name__}: {e}",
                tb=traceback.format_exc(), started=t0, log_hint=log_hint)
        logger.error("%s %s FAILED: %s\n%s", archive or "global", name, e, json.dumps(s.details, default=str, indent=2))
        raise AirflowFailException(f"{archive + ' ' if archive else ''}{name} failed: {e}") from e
    if s.warning:
        s.details["warning"] = s.warning
    status = STATUS_WARN if s.warning else STATUS_PASS
    _record(run_id, name, archive, status, details=s.details, started=t0, log_hint=log_hint)
    logger.info("%s %s %s: %s", archive or "global", name, status.upper(), json.dumps(s.details, default=str))


def _trigger_callback(status: str):
    """on_success / on_failure callback for a trigger task: records the step."""
    def cb(context: Dict[str, Any]) -> None:
        ti = context.get("ti") or context.get("task_instance")
        task_id = getattr(ti, "task_id", "") or ""
        key, stage = task_id.split("__", 1) if "__" in task_id else ("", task_id)
        label = ARCHIVES.get(key, {}).get("label", key)
        target = TRIGGERED_DAG.get(stage, "").format(key=key)
        rid = triggered_run_id(context.get("run_id", "unknown"), key, stage)
        exc = context.get("exception")
        error = None
        if status != STATUS_PASS:
            error = f"{type(exc).__name__}: {exc}" if exc else "triggered run failed"
            if exc is not None and "Timeout" in type(exc).__name__:
                error += (f" -- the triggered run did not finish in time; if it is still queued, {target} "
                          f"is probably paused: airflow dags unpause {target}")
        try:
            _record(context.get("run_id", "unknown"), stage, label, status,
                    details={"triggered_dag": target, "triggered_run_id": rid},
                    error=error,
                    tb="".join(traceback.format_exception(exc)) if exc is not None else None,
                    log_hint=f"DAG {target}, run {rid}")
        except Exception:
            logger.exception("Could not record %s %s", label, stage)
    return cb


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

def _api_url(params: Dict[str, Any]) -> str:
    return (params.get("api_url") or os.environ.get("D3_API_URL") or "http://api:8000").rstrip("/")


def preflight(**context) -> None:
    """Everything the later stages need, checked up front with a clear failure each."""
    params = context["params"]
    failed: List[str] = []

    def run(name: str, fn) -> None:
        try:
            with step(context, name) as s:
                fn(s)
        except AirflowFailException as e:
            failed.append(str(e))

    def database(s: _Step) -> None:
        with get_db_connection() as conn:
            cur = conn.cursor()
            apply_schema_ddl(cur, SCHEMA_SQL)
            conn.commit()
            cur.execute("SELECT 1")
            tables = [f"{k}_dataset" for k in ARCHIVES] + ["papers", "unified_datasets"]
            cur.execute("SELECT relname FROM pg_class WHERE relname = ANY(%s)", (tables,))
            present = {r[0] for r in cur.fetchall()}
            s.details["tables_present"] = sorted(present)
            missing = [t for t in tables if t not in present]
            if missing:
                s.warn(f"not yet created (the stages create them): {missing}")

    def openrouter(s: _Step) -> None:
        key = get_openrouter_api_key()
        validate_openrouter_api_key(key)  # raises on a missing/expired/revoked key
        model = params.get("model") or DEFAULT_MODEL
        credit = openrouter_credit_remaining(model)
        s.details.update({"model": model, "prompt_version": PROMPT_VERSION, "credit_usd": credit})
        if credit is not None and credit < 2:
            raise RuntimeError(f"OpenRouter credit ${credit:.2f} is too low to classify")

    def openalex(s: _Step) -> None:
        s.details["budget"] = check_openalex_budget({"min_openalex_requests": int(params.get("min_openalex_requests", 50))})
        if os.environ.get("OPENALEX_API_KEY") in (None, ""):
            s.warn("OPENALEX_API_KEY is not set: mapping uses the shared keyless budget")

    def fetcher(s: _Step) -> None:
        f = get_paper_fetcher()
        if f is None:
            raise RuntimeError("paper-text-fetcher is not installed in this image; full text will not be fetched")
        s.details["fetcher"] = type(f).__name__
        s.details["contact_email_set"] = bool(os.environ.get("PAPER_FETCHER_CONTACT_EMAIL"))
        if not s.details["contact_email_set"]:
            s.warn("PAPER_FETCHER_CONTACT_EMAIL is not set: Unpaywall is skipped")

    def api(s: _Step) -> None:
        url = f"{_api_url(params)}/api/datasets?limit=1"
        r = requests.get(url, timeout=30)
        s.details.update({"url": url, "http": r.status_code})
        r.raise_for_status()
        s.details["datasets_total"] = r.json().get("count")

    run("preflight_database", database)
    run("preflight_openrouter", openrouter)
    run("preflight_openalex", openalex)
    run("preflight_fetcher", fetcher)
    run("preflight_api", api)
    if failed:
        raise AirflowFailException("Preflight failed: " + " | ".join(failed))


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def _run_started(context: Dict[str, Any]) -> datetime:
    dag_run = context.get("dag_run")
    return getattr(dag_run, "start_date", None) or datetime.now(timezone.utc) - timedelta(hours=6)


def verify_ingest(*, key: str, **context) -> None:
    cfg = ARCHIVES[key]
    ds = context["params"][f"{key}_dataset_id"]
    ingest_id = context["params"].get(f"{key}_ingest_id") or ds
    with step(context, "verify_ingest", cfg["label"]) as s, get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT dataset_id, title, updated_at FROM {key}_dataset WHERE dataset_id = %s", (ds,))
        row = cur.fetchone()
        s.details["dataset_id"] = ds
        if key == "crcns" and ingest_id != ds:
            cur.execute("SELECT count(*) FROM crcns_dataset WHERE dataset_id = %s", (ingest_id,))
            still_doi = cur.fetchone()[0]
            s.details["rows_still_keyed_by_doi"] = still_doi
            if still_doi:
                raise RuntimeError(f"CRCNS {ingest_id} is still keyed by its DOI; the DOI -> {ds} re-key did not happen")
        if not row:
            raise RuntimeError(f"{key}_dataset has no row for {ds} after ingestion")
        s.details["title"] = (row[1] or "")[:120]
        cur.execute("SELECT count(*) FROM unified_datasets WHERE source = %s AND dataset_id = %s", (cfg["label"], ds))
        s.details["in_unified_datasets"] = bool(cur.fetchone()[0])
        if not s.details["in_unified_datasets"]:
            raise RuntimeError(f"{ds} is missing from unified_datasets (what the API and site read)")


def verify_map(*, key: str, **context) -> None:
    cfg = ARCHIVES[key]
    ds = context["params"][f"{key}_dataset_id"]
    since = _run_started(context)
    id_col = f"{key}_id"
    with step(context, "verify_map", cfg["label"]) as s, get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT paper_doi, resolved_at FROM {key}_paper_map WHERE {id_col} = %s", (ds,))
        maps = cur.fetchall()
        s.details["primary_papers"] = [m[0] for m in maps]
        if not maps:
            raise RuntimeError(f"no primary paper resolved for {ds}")
        s.details["resolved_this_run"] = sum(1 for m in maps if m[1] and m[1] >= since)
        if not s.details["resolved_this_run"]:
            s.warn("primary paper mapping was not refreshed by this run")
        cur.execute(f"""
            SELECT count(*), count(*) FILTER (WHERE p.text_status = 'full_text'),
                   count(*) FILTER (WHERE c.resolved_at >= %s)
            FROM {key}_paper_citations c LEFT JOIN papers p ON p.paper_doi = c.citing_paper_doi
            WHERE c.{id_col} = %s""", (since, ds))
        edges, full_text, fresh = cur.fetchone()
        s.details.update({"citation_edges": edges, "citing_with_full_text": full_text, "edges_touched_this_run": fresh})
        if not edges:
            raise RuntimeError(f"no citing papers found for {ds}'s primary paper(s)")
        if not full_text:
            s.warn("no citing paper has full text yet; classification will have nothing to read")
        cur.execute(f"SELECT papers FROM {key}_dataset WHERE dataset_id = %s", (ds,))
        s.details["papers_column"] = (cur.fetchone() or [None])[0]


def verify_classify(*, key: str, **context) -> None:
    cfg = ARCHIVES[key]
    ds = context["params"][f"{key}_dataset_id"]
    since = _run_started(context)
    id_col = f"{key}_id"
    rid = triggered_run_id(context.get("run_id", "unknown"), key, "classify")
    with step(context, "verify_classify", cfg["label"], log_hint=f"DAG paper_reuse_classification, run {rid}") as s, \
            get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT status, classification, prompt_version, classification_model, hallucinated_quote_count,
                   jsonb_array_length(COALESCE(evidence_quotes, '[]'::jsonb)), error_kind, reasoning
            FROM {key}_paper_citation_classifications
            WHERE {id_col} = %s AND classified_at >= %s""", (ds, since))
        rows = cur.fetchall()
        s.details["rows_this_run"] = len(rows)
        s.details["labels"] = {}
        for r in rows:
            k = r[1] or r[0]
            s.details["labels"][k] = s.details["labels"].get(k, 0) + 1
        classified = [r for r in rows if r[0] == "classified"]
        errors = [r for r in rows if r[0] == "error"]
        s.details["prompt_versions"] = sorted({r[2] for r in classified if r[2] is not None})
        s.details["models"] = sorted({r[3] for r in classified if r[3]})
        s.details["with_evidence_quotes"] = sum(1 for r in classified if (r[5] or 0) > 0)
        if errors:
            s.details["errors"] = [f"{r[6]}: {(r[7] or '')[:200]}" for r in errors]
        if not rows:
            raise RuntimeError(f"no classification rows written for {ds} in this run")
        if not classified:
            raise RuntimeError(f"no pair classified for {ds}: {s.details['labels']}; errors: {s.details.get('errors')}")
        if PROMPT_VERSION not in s.details["prompt_versions"]:
            raise RuntimeError(f"classified with prompt version {s.details['prompt_versions']}, expected {PROMPT_VERSION}")
        if errors:
            s.warn(f"{len(errors)} pair(s) ended in a classifier error")
        if not s.details["with_evidence_quotes"]:
            s.warn("no classification carries evidence quotes")

        # The known REUSE pair must be re-classified this run and still come
        # back REUSE with evidence: proves the classifier can still say REUSE,
        # not just that it runs.
        reuse_doi = (context["params"].get(f"{key}_reuse_citing_doi") or "").strip().lower()
        if reuse_doi:
            cur.execute(f"""
                SELECT status, classification, confidence,
                       jsonb_array_length(COALESCE(evidence_quotes, '[]'::jsonb)), reasoning
                FROM {key}_paper_citation_classifications
                WHERE {id_col} = %s AND lower(citing_paper_doi) = %s AND classified_at >= %s""",
                        (ds, reuse_doi, since))
            got = cur.fetchone()
            s.details["known_reuse_pair"] = {"citing_paper_doi": reuse_doi,
                                             "label": got[1] if got else None,
                                             "confidence": got[2] if got else None,
                                             "evidence_quotes": got[3] if got else None}
            if not got:
                raise RuntimeError(f"known REUSE pair {reuse_doi} was not re-classified this run "
                                   f"(is it still a citation edge of {ds}?)")
            if got[1] != "REUSE":
                raise RuntimeError(f"known REUSE pair {reuse_doi} came back {got[1] or got[0]} "
                                   f"(confidence {got[2]}): {(got[4] or '')[:300]}")
            if not got[3]:
                raise RuntimeError(f"known REUSE pair {reuse_doi} is REUSE but has no evidence quotes")

        cur.execute("SELECT summary FROM paper_reuse_classification_runs WHERE run_id = %s ORDER BY id DESC LIMIT 1", (rid,))
        run = cur.fetchone()
        if run and isinstance(run[0], dict):
            u = run[0].get("usage") or {}
            s.details["run_tokens"] = u.get("total_tokens")


def check_api(*, key: str, **context) -> None:
    cfg = ARCHIVES[key]
    ds = context["params"][f"{key}_dataset_id"]
    base = _api_url(context["params"])
    with step(context, "check_api", cfg["label"]) as s:
        url = f"{base}/api/datasets/{cfg['label']}/{ds}"
        r = requests.get(url, timeout=60)
        s.details.update({"url": url, "http": r.status_code})
        r.raise_for_status()
        body = r.json()
        cites = body.get("citations") or []
        labelled = [c for c in cites if c.get("classification")]
        s.details.update({
            "primary_papers": len(body.get("primary_papers") or []),
            "citations": len(cites),
            "labelled_citations": len(labelled),
            "labels": sorted({c["classification"] for c in labelled}),
        })
        if not body.get("dataset"):
            raise RuntimeError("API returned no dataset")
        if not s.details["primary_papers"]:
            raise RuntimeError("API shows no primary paper for the dataset")
        if not labelled:
            raise RuntimeError("API shows no classified citation (the API or its CTE may not read the new columns)")


def check_site(**context) -> None:
    url = (context["params"].get("site_url") or os.environ.get("D3_FRONTEND_URL") or "http://frontend:3000").rstrip("/")
    with step(context, "check_site") as s:
        r = requests.get(url + "/", timeout=60)
        s.details.update({"url": url, "http": r.status_code, "bytes": len(r.content)})
        r.raise_for_status()
        if "<div id=\"root\"" not in r.text and "id=root" not in r.text:
            s.warn("page loaded but has no React root element")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def report(**context) -> Dict[str, Any]:
    run_id = context.get("run_id", "unknown")
    with get_db_connection() as conn:
        cur = conn.cursor()
        apply_schema_ddl(cur, SCHEMA_SQL)
        cur.execute(
            "SELECT step, archive, status, details, error, log_hint FROM integration_test_steps WHERE run_id = %s",
            (run_id,))
        cols = ["step", "archive", "status", "details", "error", "log_hint"]
        steps = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
        rep = build_report(steps, ARCHIVES)
        params = context["params"]
        config = {k: params.get(k) for k in params.keys()} if hasattr(params, "keys") else {}
        cur.execute(
            """
            INSERT INTO integration_test_runs (run_id, started_at, finished_at, passed, failures, warnings, matrix, config)
            VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET finished_at = EXCLUDED.finished_at, passed = EXCLUDED.passed,
                failures = EXCLUDED.failures, warnings = EXCLUDED.warnings, matrix = EXCLUDED.matrix;
            """,
            (run_id, _run_started(context), rep["passed"], Json(rep["failures"]), Json(rep["warnings"]),
             Json(rep["matrix"]), Json(config, dumps=lambda o: json.dumps(o, default=str))),
        )
        conn.commit()
    logger.info("=== Stack integration test ===\n%s", format_report(rep))
    if not rep["passed"]:
        raise AirflowFailException("Stack integration test FAILED:\n" + "\n".join(rep["failures"]))
    return rep


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

def _params() -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "citing_papers_per_primary": Param(5, type="integer", title="Citing papers per primary paper",
                                           description="Mapping backfill cap for the test datasets."),
        "pairs_per_archive": Param(2, type="integer", title="Pairs classified per archive",
                                   description="Whole-paper LLM calls per archive (re-classified every run)."),
        "min_openalex_requests": Param(50, type="integer", title="Min OpenAlex requests left today"),
        "model": Param(DEFAULT_MODEL, type="string", title="Classification model"),
        "api_url": Param(os.environ.get("D3_API_URL", ""), type="string", title="API base URL",
                         description="Empty = $D3_API_URL, else http://api:8000 (local compose)."),
        "site_url": Param(os.environ.get("D3_FRONTEND_URL", ""), type="string", title="Site URL",
                          description="Empty = $D3_FRONTEND_URL, else http://frontend:3000 (local compose)."),
    }
    for key, cfg in ARCHIVES.items():
        p[f"{key}_dataset_id"] = Param(cfg["dataset_id"], type="string", title=f"{cfg['label']} test dataset")
        p[f"{key}_reuse_citing_doi"] = Param(
            cfg.get("reuse_citing_doi", ""), type="string", title=f"{cfg['label']} known REUSE citing paper",
            description="A citing paper known to reuse the test dataset. Classified first every run and "
                        "must come back REUSE. Empty = no REUSE assertion for this archive.")
        if cfg["ingest_id"] != cfg["dataset_id"]:
            p[f"{key}_ingest_id"] = Param(cfg["ingest_id"], type="string", title=f"{cfg['label']} id to ingest by",
                                          description="CRCNS ingestion matches the DOI, before the code is resolved.")
    return p


dag = DAG(
    DAG_ID,
    default_args={
        "owner": "neurod3",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "retries": 0,
    },
    description="Wiring test: one dataset per archive through ingestion, mapping, classification and the API",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Manual / deploy-triggered only, so unpaused is safe.
    is_paused_upon_creation=False,
    # Templated conf keeps real types (lists, ints, bools), not their string forms.
    render_template_as_native_obj=True,
    tags=["test", "integration", "pipeline"],
    params=_params(),
)

preflight_task = PythonOperator(task_id="preflight", python_callable=preflight, dag=dag)
site_task = PythonOperator(task_id="check_site", python_callable=check_site, trigger_rule="all_done", dag=dag)
report_task = PythonOperator(task_id="report", python_callable=report, trigger_rule="all_done", dag=dag)


def _trigger(key: str, stage: str, conf: Dict[str, Any], timeout_min: int) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=f"{key}__{stage}",
        trigger_dag_id=TRIGGERED_DAG[stage].format(key=key),
        trigger_run_id="it__{{ run_id }}__" + key + "__" + stage,
        conf=conf,
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
        # A paused target DAG leaves the triggered run queued, so the wait ends in
        # this timeout (fail_when_dag_is_paused is not supported on Airflow 3 yet);
        # the failure callback then says so. The deploy step unpauses these DAGs.
        execution_timeout=timedelta(minutes=timeout_min),
        on_success_callback=_trigger_callback(STATUS_PASS),
        on_failure_callback=_trigger_callback(STATUS_FAIL),
        dag=dag,
    )


for key, cfg in ARCHIVES.items():
    ds = "{{ params." + key + "_dataset_id }}"
    ingest_id = "{{ params." + key + "_ingest_id }}" if cfg["ingest_id"] != cfg["dataset_id"] else ds

    ingest = _trigger(key, "ingest", {"dataset_ids": [ingest_id]}, 30)
    v_ingest = PythonOperator(task_id=f"{key}__verify_ingest", python_callable=verify_ingest,
                              op_kwargs={"key": key}, dag=dag)
    map_ = _trigger(key, "map", {
        "dataset_ids": [ds],
        "max_citing_papers_per_primary": "{{ params.citing_papers_per_primary }}",
        "min_openalex_requests": "{{ params.min_openalex_requests }}",
        "batch_size": 1,
        "enable_citation_enrichment": True,
        "write_run_artifacts": False,
    }, 30)
    v_map = PythonOperator(task_id=f"{key}__verify_map", python_callable=verify_map, op_kwargs={"key": key}, dag=dag)
    classify = _trigger(key, "classify", {
        "source_filter": cfg["label"],
        "dataset_ids": [ds],
        "max_edges_per_run": "{{ params.pairs_per_archive }}",
        "include_citing_dois": ["{{ params." + key + "_reuse_citing_doi }}"],
        "reclassify_existing": True,
        "batch_size": 2,
        "model": "{{ params.model }}",
        "min_credit_usd": 1.0,
    }, 20)
    v_cls = PythonOperator(task_id=f"{key}__verify_classify", python_callable=verify_classify,
                           op_kwargs={"key": key}, dag=dag)
    api = PythonOperator(task_id=f"{key}__check_api", python_callable=check_api, op_kwargs={"key": key}, dag=dag)

    preflight_task >> ingest >> v_ingest >> map_ >> v_map >> classify >> v_cls >> api >> site_task

site_task >> report_task
