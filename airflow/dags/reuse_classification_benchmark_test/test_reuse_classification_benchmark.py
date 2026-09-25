"""
Tests for the reuse classification benchmark: the answer key's shape and the
pure selection / scoring helpers. No database, no network, no LLM.

Run inside the Airflow container:
    python -m pytest /opt/airflow/dags/reuse_classification_benchmark_test
"""

import os
import sys

import pytest

DAGS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

pytest.importorskip("airflow")

import reuse_classification_benchmark_dag as B  # noqa: E402
from airflow.exceptions import AirflowFailException  # noqa: E402


@pytest.fixture(scope="module")
def key():
    return B.load_answer_key()


class TestAnswerKey:
    def test_counts_match_header(self, key):
        pairs = key["pairs"]
        assert key["counts"]["full"] == len(pairs) == 161
        assert key["counts"]["smoke"] == sum(p["smoke"] for p in pairs) == 20

    def test_pair_ids_are_unique(self, key):
        ids = [p["pair_id"] for p in key["pairs"]]
        assert len(ids) == len(set(ids))

    def test_expected_follows_the_human_call(self, key):
        for p in key["pairs"]:
            assert p["expected"] == ("REUSE" if p["human_call"] == "reuse" else "NOT_REUSE")

    def test_mode_follows_the_pathway(self, key):
        for p in key["pairs"]:
            assert p["mode"] == {"indirect": "citing", "direct": "direct"}[p["pathway"]]

    def test_indirect_pairs_name_the_primary_paper(self, key):
        # The citing prompt is built around the cited paper; without it the
        # model is asked about reuse of nothing in particular.
        assert all(p["primary_paper_doi"] for p in key["pairs"] if p["pathway"] == "indirect")

    def test_smoke_set_is_balanced(self, key):
        smoke = [p for p in key["pairs"] if p["smoke"]]
        assert sum(p["expected"] == "REUSE" for p in smoke) == 10
        assert sum(p["expected"] == "NOT_REUSE" for p in smoke) == 10


class TestSelectPairs:
    PAIRS = [{"pair_id": str(i), "smoke": i % 2 == 0} for i in range(6)]

    def test_smoke_takes_only_flagged_pairs(self):
        assert [p["pair_id"] for p in B.select_pairs(self.PAIRS, "smoke")] == ["0", "2", "4"]

    def test_full_takes_everything(self):
        assert len(B.select_pairs(self.PAIRS, "full")) == 6

    def test_max_pairs_caps_the_set(self):
        assert len(B.select_pairs(self.PAIRS, "FULL", max_pairs=2)) == 2

    def test_unknown_set_is_refused(self):
        with pytest.raises(AirflowFailException):
            B.select_pairs(self.PAIRS, "everything")


class TestGroupByPaper:
    def test_a_papers_pairs_stay_in_one_batch(self):
        pairs = [{"fetched_doi": d, "pair_id": f"{d}|{i}"} for i, d in enumerate(["a", "b", "a", "c", "b"])]
        batches = B.group_by_paper(pairs, papers_per_batch=2)
        assert [[g[0]["fetched_doi"] for g in b] for b in batches] == [["a", "b"], ["c"]]
        assert len(batches[0][0]) == 2 and len(batches[0][1]) == 2


def row(expected, label=None, status="classified", pathway="indirect", reuse_type=None, types=()):
    return {"pair_id": f"{expected}-{label}-{status}", "expected": expected, "label": label, "status": status,
            "pathway": pathway, "human_call": "reuse" if expected == "REUSE" else "neither",
            "reuse_type": reuse_type, "expected_reuse_types": list(types), "confidence": 9, "reasoning": "r"}


class TestScoring:
    def test_recall_and_false_reuse_rate(self):
        rows = [row("REUSE", "REUSE"), row("REUSE", "MENTION"),
                row("NOT_REUSE", "REUSE"), row("NOT_REUSE", "MENTION"), row("NOT_REUSE", "NEITHER"),
                row("NOT_REUSE", "NEITHER")]
        m = B.score_results(rows)
        assert m["reuse_recall"] == 0.5
        assert m["false_reuse_rate"] == 0.25
        assert m["accuracy"] == round(4 / 6, 4)
        assert len(m["disagreements"]) == 2

    def test_missing_text_lowers_coverage_not_recall(self):
        rows = [row("REUSE", "REUSE"), row("REUSE", status="no_full_text"), row("NOT_REUSE", "MENTION")]
        m = B.score_results(rows)
        assert m["reuse_recall"] == 1.0
        assert m["text_coverage"] == round(2 / 3, 4)
        assert m["no_full_text"] == 1

    def test_reuse_type_agreement_counts_only_hits(self):
        rows = [row("REUSE", "REUSE", reuse_type="BENCHMARK", types=["BENCHMARK"]),
                row("REUSE", "REUSE", reuse_type="TOOL_DEMO", types=["NOVEL_ANALYSIS"]),
                row("REUSE", "MENTION", types=["BENCHMARK"])]
        assert B.score_results(rows)["reuse_type_agreement"] == 0.5

    def test_primary_is_not_reuse(self):
        m = B.score_results([row("NOT_REUSE", "PRIMARY", pathway="direct")])
        assert m["false_reuse_rate"] == 0.0

    def test_breakdown_by_pathway(self):
        m = B.score_results([row("REUSE", "REUSE", pathway="direct"), row("REUSE", "MENTION")])
        assert m["by_pathway"]["direct"]["reuse_recall"] == 1.0
        assert m["by_pathway"]["indirect"]["reuse_recall"] == 0.0


class TestThresholds:
    GOOD = {"text_coverage": 1.0, "reuse_recall": 0.9, "false_reuse_rate": 0.1, "errors": 0}

    def test_good_run_passes(self):
        assert B.check_thresholds(self.GOOD, 0.8, 0.5, 0.8) == []

    @pytest.mark.parametrize("field,value,needle", [
        ("reuse_recall", 0.5, "reuse_recall"),
        ("false_reuse_rate", 0.7, "false_reuse_rate"),
        ("text_coverage", 0.3, "text_coverage"),
        ("errors", 2, "classifier error"),
        ("reuse_recall", None, "undefined"),
    ])
    def test_each_regression_fails(self, field, value, needle):
        failures = B.check_thresholds({**self.GOOD, field: value}, 0.8, 0.5, 0.8)
        assert any(needle in f for f in failures)


class TestDag:
    def test_dag_shape(self):
        assert B.dag.dag_id == "reuse_classification_benchmark_test"
        assert set(B.dag.task_ids) == {"prepare_benchmark", "build_benchmark_batches",
                                        "classify_benchmark_batch", "score_benchmark_run"}
