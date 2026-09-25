"""
Tests for the stack integration test's report logic and DAG shape.
No database, no network, no triggered runs.

Run inside the Airflow container:
    python -m pytest /opt/airflow/dags/stack_integration_test
"""

import os
import sys

import pytest

DAGS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HERE = os.path.dirname(os.path.abspath(__file__))
for p in (DAGS_DIR, HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

pytest.importorskip("airflow")

import stack_integration_test_dag as S  # noqa: E402

TWO = {"dandi": {"label": "DANDI"}, "sparc": {"label": "SPARC"}}


def row(step, archive="", status="pass", error=None, log_hint=None, details=None):
    return {"step": step, "archive": archive, "status": status, "error": error,
            "log_hint": log_hint, "details": details or {}}


def all_global(status="pass"):
    return [row(g, status=status) for g in S.GLOBAL_STEPS]


def all_stages(label, status="pass"):
    return [row(st, label, status) for st in S.STAGES]


class TestBuildReport:
    def test_everything_passes(self):
        rep = S.build_report(all_global() + all_stages("DANDI") + all_stages("SPARC"), TWO)
        assert rep["passed"] and rep["failures"] == []
        assert set(rep["matrix"]) == {"(global)", "DANDI", "SPARC"}

    def test_failure_stops_that_archive_only(self):
        steps = all_global() + all_stages("SPARC") + [
            row("ingest", "DANDI"), row("verify_ingest", "DANDI"),
            row("map", "DANDI", "fail", error="AirflowException: triggered run failed\nmore",
                log_hint="DAG dandi_paper_mapping, run it__x__dandi__map"),
        ]
        rep = S.build_report(steps, TWO)
        assert not rep["passed"]
        assert rep["matrix"]["DANDI"]["verify_map"] == "not reached"
        assert rep["matrix"]["DANDI"]["check_api"] == "not reached"
        assert all(v == "pass" for v in rep["matrix"]["SPARC"].values())
        assert len(rep["failures"]) == 1
        # The failure names the step, the first error line, and where to look.
        assert "DANDI map: AirflowException: triggered run failed" in rep["failures"][0]
        assert "dandi_paper_mapping" in rep["failures"][0] and "more" not in rep["failures"][0]

    def test_preflight_failure_marks_every_archive_not_reached(self):
        steps = [row(g, status="fail" if g == "preflight_openrouter" else "pass", error="RuntimeError: key expired")
                 for g in S.GLOBAL_STEPS]
        rep = S.build_report(steps, TWO)
        assert not rep["passed"]
        assert rep["matrix"]["DANDI"]["ingest"] == "not reached"
        assert rep["failures"] == ["preflight_openrouter: RuntimeError: key expired"]

    def test_missing_row_without_an_earlier_failure_is_a_failure(self):
        # e.g. the task was killed before it could record anything
        steps = all_global() + all_stages("SPARC") + [row("ingest", "DANDI")]
        rep = S.build_report(steps, TWO)
        assert rep["matrix"]["DANDI"]["verify_ingest"] == "missing"
        assert rep["matrix"]["DANDI"]["map"] == "not reached"
        assert not rep["passed"]

    def test_warnings_do_not_fail_the_run(self):
        steps = all_global() + all_stages("SPARC") + [
            *all_stages("DANDI")[:-1],
            row("check_api", "DANDI", "warn", details={"warning": "no evidence quotes"}),
        ]
        rep = S.build_report(steps, TWO)
        assert rep["passed"]
        assert rep["warnings"] == ["DANDI check_api: no evidence quotes"]


def test_format_report_has_a_row_per_archive_and_the_result():
    rep = S.build_report(all_global() + all_stages("DANDI") + all_stages("SPARC"), TWO)
    text = S.format_report(rep)
    assert "DANDI" in text and "SPARC" in text and "RESULT: PASSED" in text


def test_triggered_run_id_is_traceable():
    assert S.triggered_run_id("manual__2026-09-25T10:00:00+00:00", "crcns", "map") == \
        "it__manual__2026-09-25T10:00:00+00:00__crcns__map"


class TestDag:
    def test_one_chain_per_archive_between_preflight_and_report(self):
        ids = set(S.dag.task_ids)
        assert {"preflight", "check_site", "report"} <= ids
        for key in S.ARCHIVES:
            for stage in S.STAGES:
                assert f"{key}__{stage}" in ids

    def test_triggers_target_the_real_dags(self):
        targets = {t.task_id: t.trigger_dag_id for t in S.dag.tasks if hasattr(t, "trigger_dag_id")}
        assert targets["dandi__ingest"] == "dandi_ingestion"
        assert targets["crcns__map"] == "crcns_paper_mapping"
        assert targets["sparc__classify"] == "paper_reuse_classification"

    def test_report_always_runs(self):
        assert S.dag.get_task("report").trigger_rule == "all_done"

    def test_crcns_ingests_by_doi_and_maps_by_code(self):
        assert S.ARCHIVES["crcns"]["ingest_id"].startswith("10.6080/")
        assert not S.ARCHIVES["crcns"]["dataset_id"].startswith("10.")
