"""
Tests for the stack integration test's report logic and DAG shape.
No database, no network, no triggered runs.

Run inside the Airflow container:
    python -m pytest /opt/airflow/dags/stack_integration_test
"""

import importlib
import json
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
        for d in S.ARCHIVES["crcns"]["datasets"]:
            assert d["ingest_id"].startswith("10.6080/")
            assert not d["dataset_id"].startswith("10.")

    def test_polls_triggered_runs_often(self):
        assert S.dag.get_task("dandi__map").poke_interval <= 5


FIXTURE = {
    "datasets": {k: [{"dataset_id": f"{k}-1"}, {"dataset_id": f"{k}-2"}] for k in S.ARCHIVE_LABELS},
    "pairs": [{"archive": "dandi", "dataset_id": "dandi-1", "primary_paper_doi": "10.1/P",
               "citing_paper_doi": "10.2/C", "expected": "REUSE"}],
}


class TestFixture:
    def test_shipped_fixture_has_two_datasets_per_archive(self):
        for key, cfg in S.ARCHIVES.items():
            assert len(cfg["datasets"]) == 2, key

    def test_shipped_pairs_are_human_reviewed_and_on_a_test_dataset(self):
        pairs = [p for cfg in S.ARCHIVES.values() for p in cfg["pairs"]]
        assert pairs
        for p in pairs:
            assert p["reviewer"] and p["source"]
            assert p["expected"] in S.EXPECTED_LABELS

    def test_every_expected_label_is_represented(self):
        expected = {p["expected"] for cfg in S.ARCHIVES.values() for p in cfg["pairs"]}
        assert expected == set(S.EXPECTED_LABELS)

    def test_dois_are_lower_cased_and_ingest_id_defaults(self):
        archives = S.build_archives(json.loads(json.dumps(FIXTURE)))
        p = archives["dandi"]["pairs"][0]
        assert (p["primary_paper_doi"], p["citing_paper_doi"]) == ("10.1/p", "10.2/c")
        assert archives["sparc"]["datasets"][0]["ingest_id"] == "sparc-1"

    @pytest.mark.parametrize("mutate,message", [
        (lambda f: f["datasets"].pop("crcns"), "no CRCNS test dataset"),
        (lambda f: f["pairs"][0].update(expected="SECONDARY"), "expected label"),
        (lambda f: f["pairs"][0].update(dataset_id="elsewhere"), "not a dandi test dataset"),
        (lambda f: f["pairs"][0].update(archive="kaggle"), "unknown archive"),
        (lambda f: f["pairs"][0].pop("citing_paper_doi"), "citing_paper_doi"),
    ])
    def test_bad_fixture_fails_at_parse_time(self, mutate, message):
        f = json.loads(json.dumps(FIXTURE))
        mutate(f)
        with pytest.raises(ValueError, match=message):
            S.build_archives(f)


class TestKnownPairs:
    @pytest.mark.parametrize("expected,status,label,result", [
        ("REUSE", "classified", "REUSE", "pass"),
        ("REUSE", "classified", "MENTION", "fail"),
        ("MENTION", "classified", "MENTION", "pass"),
        ("MENTION", "classified", "NEITHER", "warn"),   # both say "not reused"
        ("NEITHER", "classified", "REUSE", "fail"),
        ("NEITHER", "no_full_text", None, "warn"),      # nothing to read, not scored
        ("REUSE", "error", None, "fail"),
        ("REUSE", None, None, "fail"),                  # not re-classified this run
    ])
    def test_judge(self, expected, status, label, result):
        assert S.judge_known_pair(expected, status, label)[0] == result

    def test_seed_runs_between_mapping_and_classification(self):
        seed = S.dag.get_task("crcns__seed_known_pairs")
        assert seed.upstream_task_ids == {"crcns__verify_map"}
        assert seed.downstream_task_ids == {"crcns__classify"}

    def test_classify_puts_known_pairs_first_and_classifies_them_in_parallel(self):
        conf = S.dag.get_task("dandi__classify").conf
        known = [p["citing_paper_doi"] for p in S.ARCHIVES["dandi"]["pairs"]]
        assert conf["include_citing_dois"] == known
        assert conf["dataset_ids"] == [d["dataset_id"] for d in S.ARCHIVES["dandi"]["datasets"]]
        assert conf["max_edges_per_run"] == "{{ params.extra_pairs_per_archive + " + str(len(known)) + " }}"
        assert conf["reclassify_existing"] is True  # otherwise an already-classified pair is skipped
        assert conf["batch_size"] == 1

    def test_report_lists_every_known_pair(self):
        kp = {"archive": "DANDI", "dataset_id": "000034", "citing_paper_doi": "10.1/x", "expected": "REUSE",
              "got": "MENTION", "result": "fail", "reason": "expected REUSE, got MENTION"}
        steps = all_global() + [row(st, "DANDI") for st in S.STAGES if st != "verify_classify"]
        steps.append(row("verify_classify", "DANDI", "fail", error="RuntimeError: known pair(s) wrong",
                         details={"known_pairs": [kp]}))
        rep = S.build_report(steps, {"dandi": {"label": "DANDI"}})
        assert rep["known_pairs"] == [kp] and not rep["passed"]
        line = next(ln for ln in S.format_report(rep).splitlines() if "10.1/x" in ln)
        assert line.split()[:6] == ["fail", "DANDI", "000034", "expected", "REUSE", "got"] and "MENTION" in line


class TestFormDefaults:
    def test_url_params_are_prefilled(self):
        for name in ("api_url", "site_url", "browser_origins"):
            p = S.dag.params.get_param(name) if hasattr(S.dag.params, "get_param") else S.dag.params[name]
            assert getattr(p, "value", p), name

    def test_local_defaults_without_env(self, monkeypatch):
        for var in ("D3_API_URL", "D3_FRONTEND_URL", "D3_FRONTEND_ORIGINS"):
            monkeypatch.delenv(var, raising=False)
        mod = importlib.reload(S)
        assert (mod.DEFAULT_API_URL, mod.DEFAULT_SITE_URL, mod.DEFAULT_BROWSER_ORIGINS) ==             ("http://api:8000", "http://frontend:3000", "http://localhost:3000")

    def test_staging_env_overrides(self, monkeypatch):
        monkeypatch.setenv("D3_API_URL", "https://api.example.run.app")
        monkeypatch.setenv("D3_FRONTEND_URL", "https://site.example.run.app")
        mod = importlib.reload(S)
        assert mod.DEFAULT_API_URL == "https://api.example.run.app"
        assert mod.DEFAULT_SITE_URL == "https://site.example.run.app"
        monkeypatch.delenv("D3_API_URL")
        monkeypatch.delenv("D3_FRONTEND_URL")
        importlib.reload(S)


class TestCors:
    SITE = "https://neuro-d3-frontend-4crdc6p33a-uw.a.run.app"
    ALT = "https://neuro-d3-frontend-601000536186.us-west1.run.app"

    def test_origin_drops_path_and_keeps_port(self):
        assert S.url_origin(self.SITE + "/datasets?x=1") == self.SITE
        assert S.url_origin("http://frontend:3000/") == "http://frontend:3000"
        assert S.url_origin("not a url") == ""

    def test_site_origin_first_then_extras_deduplicated(self):
        got = S.browser_origins(self.SITE + "/", f" {self.ALT}/ ,{self.SITE},")
        assert got == [self.SITE, self.ALT]

    def test_allowed_origin_passes(self):
        assert S.cors_problem(self.ALT, 200, {"Access-Control-Allow-Origin": self.ALT}) is None
        assert S.cors_problem(self.ALT, 200, {"access-control-allow-origin": "*"}) is None

    def test_rejected_preflight_fails(self):
        # What staging did before the second hostname was allowed: 400, no header.
        assert "HTTP 400" in S.cors_problem(self.ALT, 400, {})

    def test_other_origin_echoed_fails(self):
        assert "ALLOWED_ORIGINS" in S.cors_problem(self.ALT, 200, {"Access-Control-Allow-Origin": self.SITE})

    def test_cors_task_reports_even_if_the_pipeline_fails(self):
        t = S.dag.get_task("check_cors")
        assert t.trigger_rule == "all_done"
        assert "report" in t.downstream_task_ids
        assert "check_cors" in S.GLOBAL_STEPS


class TestMissingGlobalStep:
    @pytest.mark.parametrize("missing", ["check_site", "check_cors", "preflight_api"])
    def test_a_killed_global_check_fails_the_run(self, missing):
        steps = [row(g) for g in S.GLOBAL_STEPS if g != missing] + all_stages("DANDI") + all_stages("SPARC")
        rep = S.build_report(steps, TWO)
        assert rep["matrix"]["(global)"][missing] == "missing"
        assert not rep["passed"]
        assert any(f.startswith(f"{missing}: no result recorded") for f in rep["failures"])


class TestWarningsAreVisible:
    """A warning passes the check but ends the task as skipped, not green."""

    @pytest.fixture
    def recorded(self, monkeypatch):
        rows = []
        monkeypatch.setattr(S, "_record", lambda run_id, name, archive, status, **k: rows.append((name, status)))
        return rows

    def test_warning_is_recorded_then_skips_the_task(self, recorded):
        with pytest.raises(S.AirflowSkipException, match="WARNING DANDI verify_map: stale"):
            with S.step({"run_id": "r"}, "verify_map", "DANDI") as s:
                s.warn("stale")
        assert recorded == [("verify_map", "warn")]

    def test_a_clean_check_passes(self, recorded):
        with S.step({"run_id": "r"}, "verify_map", "DANDI"):
            pass
        assert recorded == [("verify_map", "pass")]

    def test_a_multi_check_task_can_defer_the_skip(self, recorded):
        with S.step({"run_id": "r"}, "preflight_openalex", skip_on_warn=False) as s:
            s.warn("no key")
        assert recorded == [("preflight_openalex", "warn")]

    def test_the_chain_continues_after_a_warning_but_not_after_a_failure(self):
        for key in S.ARCHIVES:
            for stage in S.STAGES:
                if stage == "ingest":
                    continue  # its upstream is preflight, which can also end skipped
                assert S.dag.get_task(f"{key}__{stage}").trigger_rule == "none_failed", (key, stage)
            assert S.dag.get_task(f"{key}__ingest").trigger_rule == "none_failed"

    def test_report_says_passed_with_warnings(self):
        steps = all_global() + all_stages("DANDI") + all_stages("SPARC")
        steps[-1] = row(steps[-1]["step"], "SPARC", "warn", details={"warning": "only no full text"})
        rep = S.build_report(steps, TWO)
        assert rep["passed"] and rep["warnings"]
        assert "RESULT: PASSED WITH 1 WARNING(S)" in S.format_report(rep)

    def test_clean_report_says_passed(self):
        rep = S.build_report(all_global() + all_stages("DANDI") + all_stages("SPARC"), TWO)
        assert "RESULT: PASSED\n" in S.format_report(rep) + "\n"


class TestCheckApi:
    class Resp:
        def __init__(self, body, status=200):
            self.status_code, self._body = status, body

        def json(self):
            return self._body

    @pytest.fixture
    def run(self, monkeypatch):
        monkeypatch.setattr(S, "_record", lambda *a, **k: None)
        monkeypatch.setitem(S.ARCHIVES, "dandi", {"label": "DANDI", "datasets": [{"dataset_id": "1"}], "pairs": []})

        def go(citations):
            body = {"dataset": {"id": "1"}, "primary_papers": [{"doi": "10.1/p"}], "citations": citations}
            monkeypatch.setattr(S.requests, "get", lambda *a, **k: self.Resp(body))
            S.check_api(key="dandi", run_id="r", params={"api_url": "http://api"})
        return go

    def test_a_real_label_passes(self, run):
        run([{"classification": "MENTION", "classification_status": "MENTION"}])

    def test_only_no_full_text_warns(self, run):
        with pytest.raises(S.AirflowSkipException, match="only 'no full text'"):
            run([{"classification": None, "classification_status": "no_full_text"}])

    @pytest.mark.parametrize("status", ["error", "placeholder", "unclassified"])
    def test_errors_and_unattempted_rows_fail(self, run, status):
        with pytest.raises(S.AirflowFailException, match="no labelled citation"):
            run([{"classification": None, "classification_status": status}])
