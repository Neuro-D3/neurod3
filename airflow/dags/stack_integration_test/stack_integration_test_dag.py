"""
Stack integration test DAG (``stack_integration_test``).

Proves the whole pipeline is wired together, on two test datasets per archive
(DANDI, OpenNeuro, CRCNS, SPARC), listed in ``known_pairs.json``:

    preflight ─▶ ingest ─▶ verify ─▶ map papers (+ small citing-paper backfill)
              ─▶ verify ─▶ add the known pairs ─▶ classify (real LLM calls)
              ─▶ verify labels ─▶ API check ─▶ site check ─▶ report
    preflight ─▶ CORS check (browser preflight from each frontend origin) ─▶ report

Each stage triggers the real, deployed DAG (``<archive>_ingestion``,
``<archive>_paper_mapping``, ``paper_reuse_classification``) on just the test
datasets via its ``dataset_ids`` param and waits for it, so scheduling, pools,
the image and task wiring are exercised, not only the Python. The four
archives run side by side; a failure stops that archive's chain and the others
carry on.

Known pairs: human-reviewed (citing paper, dataset) pairs with an expected
label (REUSE, MENTION or NEITHER). After mapping, any the mapping did not find
are added to the citation table (``citation_source = 'stack_test_fixture'``),
then all are re-classified. A known REUSE pair that comes back as anything
else, or a known non-REUSE pair that comes back REUSE, fails the run; MENTION
vs NEITHER, or no full text to read, is a warning. Accuracy over many pairs is
the benchmark DAG's job, not this one's.

Every step writes a row to ``integration_test_steps`` (stage, archive, status,
details, duration, error and traceback, and which triggered DAG run to look
at). ``report`` runs last whatever happened, logs a one-screen report with a
line per known pair, records the run in ``integration_test_runs`` and fails
the run naming every failed step.

Trigger it by hand from the UI (the DAGs it drives must be unpaused). The API,
site and browser-origin params default to $D3_API_URL / $D3_FRONTEND_URL /
$D3_FRONTEND_ORIGINS (set on staging), else to the local compose services.
"""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlsplit

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
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

# Test datasets and known pairs. ingest_id is what the ingestion DAG matches
# on (CRCNS listings carry the DOI before the short code is resolved).
FIXTURE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "known_pairs.json")
ARCHIVE_LABELS = {"dandi": "DANDI", "openneuro": "OpenNeuro", "crcns": "CRCNS", "sparc": "SPARC"}
EXPECTED_LABELS = ("REUSE", "MENTION", "NEITHER")
# citation_source / papers.source / doi_source of rows this test adds.
FIXTURE_SOURCE = "stack_test_fixture"


def build_archives(fixture: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    archive key -> {label, datasets: [{dataset_id, ingest_id}], pairs: [...]},
    from the fixture, checked so a bad edit fails at parse time, not mid-run.
    """
    archives: Dict[str, Dict[str, Any]] = {}
    for key, label in ARCHIVE_LABELS.items():
        datasets = list((fixture.get("datasets") or {}).get(key) or [])
        if not datasets:
            raise ValueError(f"known_pairs.json lists no {label} test dataset")
        for d in datasets:
            if not d.get("dataset_id"):
                raise ValueError(f"{label} test dataset without a dataset_id: {d}")
            d.setdefault("ingest_id", d["dataset_id"])
        archives[key] = {"label": label, "datasets": datasets, "pairs": []}
    for p in fixture.get("pairs") or []:
        key = p.get("archive")
        if key not in archives:
            raise ValueError(f"known pair for unknown archive {key!r}: {p}")
        if p.get("expected") not in EXPECTED_LABELS:
            raise ValueError(f"known pair expected label must be one of {EXPECTED_LABELS}: {p}")
        if p.get("dataset_id") not in {d["dataset_id"] for d in archives[key]["datasets"]}:
            raise ValueError(f"known pair's dataset {p.get('dataset_id')!r} is not a {key} test dataset")
        for field in ("primary_paper_doi", "citing_paper_doi"):
            if not p.get(field):
                raise ValueError(f"known pair without {field}: {p}")
            p[field] = p[field].strip().lower()
        archives[key]["pairs"].append(p)
    return archives


def load_fixture(path: str = FIXTURE_PATH) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


ARCHIVES: Dict[str, Dict[str, Any]] = build_archives(load_fixture())

# Per-archive stages in order; used to lay out the report.
STAGES = ["ingest", "verify_ingest", "map", "verify_map", "seed_known_pairs", "classify", "verify_classify",
          "check_api"]
GLOBAL_STEPS = ["preflight_database", "preflight_openrouter", "preflight_openalex", "preflight_fetcher",
                "preflight_api", "check_site", "check_cors"]

# Triggered DAG for each trigger stage.
TRIGGERED_DAG = {"ingest": "{key}_ingestion", "map": "{key}_paper_mapping", "classify": "paper_reuse_classification"}

STATUS_PASS, STATUS_FAIL, STATUS_WARN = "pass", "fail", "warn"


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in test_stack_integration.py)
# ---------------------------------------------------------------------------

def triggered_run_id(run_id: str, key: str, stage: str) -> str:
    """Run id given to a triggered DAG run, so the report can point at it."""
    return f"it__{run_id}__{key}__{stage}"


def url_origin(url: str) -> str:
    """scheme://host[:port] of a URL, the form a browser sends in its Origin header."""
    p = urlsplit(url.strip())
    return f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else ""


def browser_origins(site_url: str, extra: str) -> List[str]:
    """
    Origins the API must accept: the site's own plus any comma-separated extras.
    Cloud Run serves one service on two hostnames and the browser sends whichever
    the user opened, so staging lists both.
    """
    out: List[str] = []
    for o in [url_origin(site_url)] + [url_origin(x) for x in extra.split(",")]:
        if o and o not in out:
            out.append(o)
    return out


def cors_problem(origin: str, status: int, headers: Dict[str, str]) -> Optional[str]:
    """Why a browser at `origin` would be blocked by this preflight response, or None."""
    allowed = {k.lower(): v for k, v in headers.items()}.get("access-control-allow-origin")
    if status >= 400:
        return f"preflight from {origin} returned HTTP {status} (origin not in the API's ALLOWED_ORIGINS?)"
    if allowed not in (origin, "*"):
        return f"preflight from {origin} got Access-Control-Allow-Origin={allowed!r}; add it to ALLOWED_ORIGINS"
    return None


def judge_known_pair(expected: str, status: Optional[str], label: Optional[str]) -> Tuple[str, str]:
    """
    (pass | warn | fail, reason) for one known pair's result this run.

    ``status`` / ``label`` are None when the pair was not re-classified. A wrong
    side of REUSE fails; MENTION vs NEITHER only warns (both say "not reused");
    no full text warns, since there was nothing to read.
    """
    if status is None:
        return STATUS_FAIL, "not re-classified this run"
    if status == "no_full_text":
        return STATUS_WARN, "not scored: no full text for the citing paper"
    if status != "classified" or not label:
        return STATUS_FAIL, f"classifier returned {status} instead of a label"
    if expected == "REUSE":
        return (STATUS_PASS, "REUSE as expected") if label == "REUSE" else (STATUS_FAIL, f"expected REUSE, got {label}")
    if label == "REUSE":
        return STATUS_FAIL, f"expected {expected}, got REUSE"
    if label == expected:
        return STATUS_PASS, f"{label} as expected"
    return STATUS_WARN, f"expected {expected}, got {label} (both not reused)"


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
        elif status == "missing":
            # A killed or never-run global check must not read as a pass.
            failures.append(f"{step}: no result recorded (task killed or never ran)")
        elif status == STATUS_WARN:
            warnings.append(f"{step}: {(s.get('details') or {}).get('warning', '')}")

    preflight_failed = any(matrix["(global)"].get(g) == STATUS_FAIL for g in GLOBAL_STEPS if g.startswith("preflight"))
    for cfg in archives.values():
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

    known_pairs = [kp for s in steps if s["step"] == "verify_classify"
                   for kp in ((s.get("details") or {}).get("known_pairs") or [])]
    return {"matrix": matrix, "failures": failures, "warnings": warnings, "passed": not failures,
            "known_pairs": known_pairs}


def format_report(report: Dict[str, Any]) -> str:
    """A fixed-width table, one line per archive, for the task log."""
    cols = STAGES
    short = {"ingest": "ingest", "verify_ingest": "v_ing", "map": "map", "verify_map": "v_map",
             "seed_known_pairs": "seed", "classify": "classify", "verify_classify": "v_cls", "check_api": "api"}
    lines = ["archive    " + " ".join(f"{short[c]:<11}" for c in cols)]
    for label, row in report["matrix"].items():
        if label == "(global)":
            continue
        lines.append(f"{label:<10} " + " ".join(f"{row.get(c, ''):<11}" for c in cols))
    glob = report["matrix"].get("(global)", {})
    lines.append("global: " + ", ".join(f"{k}={v}" for k, v in glob.items()))
    if report.get("known_pairs"):
        lines.append("known pairs:")
        for kp in report["known_pairs"]:
            lines.append(f"  {kp.get('result', '?'):<5} {kp.get('archive', ''):<10} {kp.get('dataset_id', ''):<9} "
                         f"expected {kp.get('expected', ''):<8} got {kp.get('got') or '-':<13} "
                         f"{kp.get('citing_paper_doi', '')}  ({kp.get('reason', '')})")
    if not report["passed"]:
        result = f"FAILED ({len(report['failures'])} failing step(s), {len(report['warnings'])} warning(s))"
    elif report["warnings"]:
        result = f"PASSED WITH {len(report['warnings'])} WARNING(S): review them below"
    else:
        result = "PASSED"
    lines.append("RESULT: " + result)
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
def step(context: Dict[str, Any], name: str, archive: str = "", log_hint: Optional[str] = None,
         skip_on_warn: bool = True) -> Iterator[_Step]:
    """
    Run one check and record it. Any exception is recorded with its traceback
    and re-raised as AirflowFailException (a check failing is not worth a
    retry). A check can also call `s.warn()`: the step is recorded as a
    warning and the task ends as *skipped* (pink in the grid, not green), so a
    warning is visible; the tasks after it use trigger_rule none_failed and
    still run. ``skip_on_warn=False`` records the warning without skipping,
    for a caller that runs several checks in one task.
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
    if s.warning and skip_on_warn:
        raise AirflowSkipException(f"WARNING {archive + ' ' if archive else ''}{name}: {s.warning}")


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
    return (params.get("api_url") or DEFAULT_API_URL).rstrip("/")


def preflight(**context) -> None:
    """Everything the later stages need, checked up front with a clear failure each."""
    params = context["params"]
    failed: List[str] = []
    warned: List[str] = []

    def run(name: str, fn) -> None:
        try:
            with step(context, name, skip_on_warn=False) as s:
                fn(s)
            if s.warning:
                warned.append(f"{name}: {s.warning}")
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
    if warned:
        raise AirflowSkipException("WARNING preflight: " + " | ".join(warned))


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def _run_started(context: Dict[str, Any]) -> datetime:
    dag_run = context.get("dag_run")
    return getattr(dag_run, "start_date", None) or datetime.now(timezone.utc) - timedelta(hours=6)


def _dataset_ids(key: str) -> List[str]:
    return [d["dataset_id"] for d in ARCHIVES[key]["datasets"]]


def verify_ingest(*, key: str, **context) -> None:
    cfg = ARCHIVES[key]
    with step(context, "verify_ingest", cfg["label"]) as s, get_db_connection() as conn:
        cur = conn.cursor()
        problems = []
        s.details["datasets"] = {}
        for d in cfg["datasets"]:
            ds, ingest_id = d["dataset_id"], d["ingest_id"]
            info: Dict[str, Any] = {}
            s.details["datasets"][ds] = info
            if key == "crcns" and ingest_id != ds:
                cur.execute("SELECT count(*) FROM crcns_dataset WHERE dataset_id = %s", (ingest_id,))
                info["rows_still_keyed_by_doi"] = cur.fetchone()[0]
                if info["rows_still_keyed_by_doi"]:
                    problems.append(f"CRCNS {ingest_id} is still keyed by its DOI; the DOI -> {ds} re-key did not happen")
            cur.execute(f"SELECT title FROM {key}_dataset WHERE dataset_id = %s", (ds,))
            row = cur.fetchone()
            if not row:
                problems.append(f"{key}_dataset has no row for {ds} after ingestion")
                continue
            info["title"] = (row[0] or "")[:120]
            cur.execute("SELECT count(*) FROM unified_datasets WHERE source = %s AND dataset_id = %s", (cfg["label"], ds))
            info["in_unified_datasets"] = bool(cur.fetchone()[0])
            if not info["in_unified_datasets"]:
                problems.append(f"{ds} is missing from unified_datasets (what the API and site read)")
        if problems:
            raise RuntimeError("; ".join(problems))


def verify_map(*, key: str, **context) -> None:
    cfg = ARCHIVES[key]
    since = _run_started(context)
    id_col = f"{key}_id"
    with step(context, "verify_map", cfg["label"]) as s, get_db_connection() as conn:
        cur = conn.cursor()
        problems, stale, unresolved = [], [], []
        seeded = {p["dataset_id"] for p in cfg["pairs"]}
        s.details["datasets"] = {}
        for ds in _dataset_ids(key):
            info: Dict[str, Any] = {}
            s.details["datasets"][ds] = info
            cur.execute(f"SELECT paper_doi, resolved_at FROM {key}_paper_map WHERE {id_col} = %s", (ds,))
            maps = cur.fetchall()
            info["primary_papers"] = [m[0] for m in maps]
            if not maps:
                # A dataset whose known pair supplies the primary paper is still
                # testable (seed_known_pairs adds it); mapping is only broken if
                # no test dataset of the archive got one.
                (unresolved if ds in seeded else problems).append(f"no primary paper resolved for {ds}")
                continue
            if not any(m[1] and m[1] >= since for m in maps):
                stale.append(ds)
            cur.execute(f"""
                SELECT count(*), count(*) FILTER (WHERE p.text_status = 'full_text'),
                       count(*) FILTER (WHERE c.resolved_at >= %s)
                FROM {key}_paper_citations c LEFT JOIN papers p ON p.paper_doi = c.citing_paper_doi
                WHERE c.{id_col} = %s""", (since, ds))
            edges, full_text, fresh = cur.fetchone()
            info.update({"citation_edges": edges, "citing_with_full_text": full_text, "edges_touched_this_run": fresh})
            if not edges:
                problems.append(f"no citing papers found for {ds}'s primary paper(s)")
        if len(unresolved) == len(cfg["datasets"]):
            problems.append("mapping resolved no primary paper for any test dataset")
        if problems:
            raise RuntimeError("; ".join(problems + unresolved))
        if unresolved:
            s.warn("; ".join(unresolved) + " (the known pair's primary paper is added before classification)")
        if stale:
            s.warn(f"primary paper mapping was not refreshed by this run for {stale}")


def seed_known_pairs(*, key: str, **context) -> None:
    """
    Make sure every known pair is a citation edge. Mapping with a small
    citing-paper cap may not reach it; the pair is added (source
    'stack_test_fixture') so the label check always has it. Rows mapping found
    itself are left as they are.
    """
    cfg = ARCHIVES[key]
    id_col = f"{key}_id"
    rid = context.get("run_id", "unknown")
    with step(context, "seed_known_pairs", cfg["label"]) as s, get_db_connection() as conn:
        cur = conn.cursor()
        s.details["pairs"] = []
        for p in cfg["pairs"]:
            ds, primary, citing = p["dataset_id"], p["primary_paper_doi"], p["citing_paper_doi"]
            cur.execute(f"""SELECT 1 FROM {key}_paper_citations
                            WHERE {id_col} = %s AND lower(primary_paper_doi) = %s AND lower(citing_paper_doi) = %s""",
                        (ds, primary, citing))
            found = cur.fetchone() is not None
            if not found:
                for doi in (primary, citing):
                    cur.execute("INSERT INTO papers (paper_doi, source) VALUES (%s, %s) ON CONFLICT (paper_doi) DO NOTHING",
                                (doi, FIXTURE_SOURCE))
                cur.execute(f"""INSERT INTO {key}_paper_map ({id_col}, paper_doi, doi_source, run_id)
                                VALUES (%s, %s, %s, %s) ON CONFLICT ({id_col}, paper_doi) DO NOTHING""",
                            (ds, primary, FIXTURE_SOURCE, rid))
                cur.execute(f"""INSERT INTO {key}_paper_citations
                                    ({id_col}, primary_paper_doi, citing_paper_doi, citation_source, run_id)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT ({id_col}, primary_paper_doi, citing_paper_doi) DO NOTHING""",
                            (ds, primary, citing, FIXTURE_SOURCE, rid))
            s.details["pairs"].append({"dataset_id": ds, "citing_paper_doi": citing, "expected": p["expected"],
                                       "found_by_mapping": found})
        conn.commit()
        if not cfg["pairs"]:
            s.warn("no known pairs for this archive; its labels are not checked")


def verify_classify(*, key: str, **context) -> None:
    cfg = ARCHIVES[key]
    since = _run_started(context)
    id_col = f"{key}_id"
    rid = triggered_run_id(context.get("run_id", "unknown"), key, "classify")
    hint = f"DAG paper_reuse_classification, run {rid}"
    with step(context, "verify_classify", cfg["label"], log_hint=hint) as s, get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT status, classification, prompt_version, classification_model, hallucinated_quote_count,
                   jsonb_array_length(COALESCE(evidence_quotes, '[]'::jsonb)), error_kind, reasoning
            FROM {key}_paper_citation_classifications
            WHERE {id_col} = ANY(%s) AND classified_at >= %s""", (_dataset_ids(key), since))
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
            raise RuntimeError(f"no classification rows written for {_dataset_ids(key)} in this run")
        if classified and PROMPT_VERSION not in s.details["prompt_versions"]:
            raise RuntimeError(f"classified with prompt version {s.details['prompt_versions']}, expected {PROMPT_VERSION}")

        # Each known pair must have been re-classified this run, on the right
        # side of REUSE (see judge_known_pair).
        failed, warned = [], []
        s.details["known_pairs"] = []
        for p in cfg["pairs"]:
            cur.execute(f"""
                SELECT status, classification, confidence, jsonb_array_length(COALESCE(evidence_quotes, '[]'::jsonb)),
                       reasoning
                FROM {key}_paper_citation_classifications
                WHERE {id_col} = %s AND lower(primary_paper_doi) = %s AND lower(citing_paper_doi) = %s
                  AND classified_at >= %s""", (p["dataset_id"], p["primary_paper_doi"], p["citing_paper_doi"], since))
            got = cur.fetchone()
            result, reason = judge_known_pair(p["expected"], got[0] if got else None, got[1] if got else None)
            if result == STATUS_PASS and p["expected"] == "REUSE" and not (got[3] or 0):
                result, reason = STATUS_FAIL, "REUSE but no evidence quotes"
            s.details["known_pairs"].append({
                "archive": cfg["label"], "dataset_id": p["dataset_id"], "citing_paper_doi": p["citing_paper_doi"],
                "expected": p["expected"], "got": (got[1] or got[0]) if got else None,
                "confidence": got[2] if got else None, "result": result, "reason": reason,
                "reasoning": ((got[4] or "")[:300] if got else None)})
            line = f"{p['dataset_id']} <- {p['citing_paper_doi']}: {reason}"
            if result == STATUS_FAIL:
                failed.append(line)
            elif result == STATUS_WARN:
                warned.append(line)
        if failed:
            raise RuntimeError("known pair(s) wrong: " + "; ".join(failed))
        if warned:
            s.warn("; ".join(warned))
        if errors:
            s.warn(f"{len(errors)} pair(s) ended in a classifier error")

        cur.execute("SELECT summary FROM paper_reuse_classification_runs WHERE run_id = %s ORDER BY id DESC LIMIT 1", (rid,))
        run = cur.fetchone()
        if run and isinstance(run[0], dict):
            u = run[0].get("usage") or {}
            s.details["run_tokens"] = u.get("total_tokens")


def check_api(*, key: str, **context) -> None:
    cfg = ARCHIVES[key]
    base = _api_url(context["params"])
    with step(context, "check_api", cfg["label"]) as s:
        problems: List[str] = []
        unlabelled: List[str] = []
        s.details["datasets"] = {}
        for ds in _dataset_ids(key):
            url = f"{base}/api/datasets/{cfg['label']}/{ds}"
            r = requests.get(url, timeout=60)
            info: Dict[str, Any] = {"url": url, "http": r.status_code}
            s.details["datasets"][ds] = info
            if r.status_code != 200:
                problems.append(f"{ds}: HTTP {r.status_code}")
                continue
            body = r.json()
            cites = body.get("citations") or []
            labelled = [c for c in cites if (c.get("classification") or "").strip()]
            no_text = [c for c in cites if c.get("classification_status") == "no_full_text"]
            info.update({"primary_papers": len(body.get("primary_papers") or []), "citations": len(cites),
                         "labelled_citations": len(labelled), "no_full_text_citations": len(no_text),
                         "labels": sorted({c["classification"] for c in labelled})})
            if not body.get("dataset"):
                problems.append(f"{ds}: API returned no dataset")
            elif not info["primary_papers"]:
                problems.append(f"{ds}: API shows no primary paper")
            elif not labelled and no_text:
                # Classified, but only "no full text": the API shows the rows,
                # there is just no label to show yet.
                unlabelled.append(ds)
            elif not labelled:
                problems.append(f"{ds}: API shows no labelled citation (does it read the new columns?)")
        if problems:
            raise RuntimeError("; ".join(problems))
        if unlabelled:
            s.warn(f"no labelled citation for {unlabelled}, only 'no full text'")


def _site_url(params: Dict[str, Any]) -> str:
    return (params.get("site_url") or DEFAULT_SITE_URL).rstrip("/")


def check_cors(**context) -> None:
    """
    The API answers server-side calls whatever its CORS settings, so check_api
    passes even when every browser request is blocked. Send the preflight a
    browser would, from each frontend origin.
    """
    params = context["params"]
    base = _api_url(params)
    origins = browser_origins(_site_url(params), params.get("browser_origins") or DEFAULT_BROWSER_ORIGINS)
    with step(context, "check_cors") as s:
        url = f"{base}/api/datasets"
        s.details.update({"url": url, "origins": {}})
        problems = []
        for origin in origins:
            r = requests.options(url, timeout=30, headers={
                "Origin": origin, "Access-Control-Request-Method": "GET"})
            s.details["origins"][origin] = {"http": r.status_code,
                                           "allow_origin": r.headers.get("access-control-allow-origin")}
            problem = cors_problem(origin, r.status_code, dict(r.headers))
            if problem:
                problems.append(problem)
        if problems:
            raise RuntimeError("; ".join(problems))


def check_site(**context) -> None:
    url = _site_url(context["params"])
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
    if rep["warnings"]:
        # Skipped, not green: the run passed but something needs a look.
        raise AirflowSkipException(f"Stack integration test PASSED WITH {len(rep['warnings'])} WARNING(S):\n"
                                   + "\n".join(rep["warnings"]))
    return rep


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

# Where the test reaches the API and site. Staging sets these in
# docker-compose.gce.yml; locally they fall back to the compose services, so
# the trigger form is always pre-filled.
DEFAULT_API_URL = os.environ.get("D3_API_URL") or "http://api:8000"
DEFAULT_SITE_URL = os.environ.get("D3_FRONTEND_URL") or "http://frontend:3000"
DEFAULT_BROWSER_ORIGINS = os.environ.get("D3_FRONTEND_ORIGINS") or "http://localhost:3000"


def _params() -> Dict[str, Any]:
    return {
        "citing_papers_per_primary": Param(10, type="integer", title="Citing papers per primary paper",
                                           description="Mapping backfill cap for the test datasets."),
        "extra_pairs_per_archive": Param(1, type="integer", title="Mapped pairs classified per archive",
                                         description="Pairs found by mapping, classified on top of the known pairs "
                                                     "(whole-paper LLM calls, re-done every run)."),
        "min_openalex_requests": Param(50, type="integer", title="Min OpenAlex requests left today"),
        "model": Param(DEFAULT_MODEL, type="string", title="Classification model"),
        "api_url": Param(DEFAULT_API_URL, type="string", title="API base URL",
                         description="Defaults to $D3_API_URL (staging), else http://api:8000 (local compose)."),
        "site_url": Param(DEFAULT_SITE_URL, type="string", title="Site URL",
                          description="Defaults to $D3_FRONTEND_URL (staging), else http://frontend:3000."),
        "browser_origins": Param(DEFAULT_BROWSER_ORIGINS, type="string", title="Extra browser origins",
                                 description="Comma-separated frontend URLs the API must allow besides the site URL "
                                             "(Cloud Run serves each service on two hostnames). Defaults to "
                                             "$D3_FRONTEND_ORIGINS, else http://localhost:3000."),
    }


dag = DAG(
    DAG_ID,
    default_args={
        "owner": "neurod3",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "retries": 0,
    },
    description="Wiring test: two datasets per archive through ingestion, mapping, classification, the API "
                "and the site, with known reuse / non-reuse pairs checked",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Triggered by hand only, so unpaused is safe.
    is_paused_upon_creation=False,
    # Templated conf keeps real types (lists, ints, bools), not their string forms.
    render_template_as_native_obj=True,
    tags=["test", "integration", "pipeline"],
    params=_params(),
)

preflight_task = PythonOperator(task_id="preflight", python_callable=preflight, dag=dag)
site_task = PythonOperator(task_id="check_site", python_callable=check_site, trigger_rule="all_done", dag=dag)
cors_task = PythonOperator(task_id="check_cors", python_callable=check_cors, trigger_rule="all_done", dag=dag)
report_task = PythonOperator(task_id="report", python_callable=report, trigger_rule="all_done", dag=dag)


# Tasks in an archive's chain run when every upstream succeeded or was skipped
# (a warning), and stop after a failure.
CHAIN_TRIGGER_RULE = "none_failed"


def _trigger(key: str, stage: str, conf: Dict[str, Any], timeout_min: int) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=f"{key}__{stage}",
        trigger_dag_id=TRIGGERED_DAG[stage].format(key=key),
        trigger_run_id="it__{{ run_id }}__" + key + "__" + stage,
        conf=conf,
        wait_for_completion=True,
        # The triggered runs take seconds to minutes; a long poke adds dead time
        # to every stage.
        poke_interval=5,
        allowed_states=["success"],
        failed_states=["failed"],
        # A paused target DAG leaves the triggered run queued, so the wait ends in
        # this timeout (fail_when_dag_is_paused is not supported on Airflow 3 yet);
        # the failure callback then says so. Unpause these DAGs before a run.
        execution_timeout=timedelta(minutes=timeout_min),
        on_success_callback=_trigger_callback(STATUS_PASS),
        on_failure_callback=_trigger_callback(STATUS_FAIL),
        # Runs after an upstream check that ended skipped (a warning).
        trigger_rule=CHAIN_TRIGGER_RULE,
        dag=dag,
    )


for key, cfg in ARCHIVES.items():
    dataset_ids = [d["dataset_id"] for d in cfg["datasets"]]
    known_citing = [p["citing_paper_doi"] for p in cfg["pairs"]]

    ingest = _trigger(key, "ingest", {"dataset_ids": [d["ingest_id"] for d in cfg["datasets"]]}, 30)
    v_ingest = PythonOperator(task_id=f"{key}__verify_ingest", python_callable=verify_ingest,
                              op_kwargs={"key": key}, trigger_rule=CHAIN_TRIGGER_RULE, dag=dag)
    map_ = _trigger(key, "map", {
        "dataset_ids": dataset_ids,
        "max_citing_papers_per_primary": "{{ params.citing_papers_per_primary }}",
        "min_openalex_requests": "{{ params.min_openalex_requests }}",
        "batch_size": 1,
        "enable_citation_enrichment": True,
        "write_run_artifacts": False,
    }, 30)
    v_map = PythonOperator(task_id=f"{key}__verify_map", python_callable=verify_map, op_kwargs={"key": key},
                           trigger_rule=CHAIN_TRIGGER_RULE, dag=dag)
    seed = PythonOperator(task_id=f"{key}__seed_known_pairs", python_callable=seed_known_pairs,
                          op_kwargs={"key": key}, trigger_rule=CHAIN_TRIGGER_RULE, dag=dag)
    classify = _trigger(key, "classify", {
        "source_filter": cfg["label"],
        "dataset_ids": dataset_ids,
        # The known pairs first, then a few pairs mapping found.
        "include_citing_dois": known_citing,
        "max_edges_per_run": "{{ params.extra_pairs_per_archive + " + str(len(known_citing)) + " }}",
        "reclassify_existing": True,
        # One paper per task, so the pairs are classified in parallel.
        "batch_size": 1,
        "model": "{{ params.model }}",
        "min_credit_usd": 1.0,
    }, 20)
    v_cls = PythonOperator(task_id=f"{key}__verify_classify", python_callable=verify_classify,
                           op_kwargs={"key": key}, trigger_rule=CHAIN_TRIGGER_RULE, dag=dag)
    api = PythonOperator(task_id=f"{key}__check_api", python_callable=check_api, op_kwargs={"key": key},
                         trigger_rule=CHAIN_TRIGGER_RULE, dag=dag)

    preflight_task >> ingest >> v_ingest >> map_ >> v_map >> seed >> classify >> v_cls >> api >> site_task

site_task >> report_task
# Independent of the pipeline, so it reports even when an archive fails early.
preflight_task >> cors_task >> report_task
