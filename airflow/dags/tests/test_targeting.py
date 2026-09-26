"""
Tests for dataset_ids targeting (utils/targeting.py) and that every DAG the
stack integration test drives exposes the param. No database, no network.
"""

import pytest

from utils.targeting import LIST_ALL, keep_requested, requested_dataset_ids, sql_id_list


class TestRequestedIds:
    @pytest.mark.parametrize("raw,expected", [
        (None, []),
        ([], []),
        ("", []),
        (["000402"], ["000402"]),
        ("000402, ds004315  157", ["000402", "ds004315", "157"]),
        (["a", " a ", "b", ""], ["a", "b"]),
        (402, ["402"]),
    ])
    def test_normalizes(self, raw, expected):
        assert requested_dataset_ids({"dataset_ids": raw}) == expected

    def test_missing_param_means_normal_run(self):
        assert requested_dataset_ids({}) == []
        assert requested_dataset_ids(None) == []


class TestKeepRequested:
    LISTED = [
        {"dataset_id": "000402"},
        {"dataset_id": "10.6080/K0MS3QNT", "doi": "10.6080/k0ms3qnt"},  # CRCNS before enrichment
        {"dataset_id": "000070"},
    ]

    def test_no_request_keeps_everything(self):
        assert keep_requested(self.LISTED, []) == self.LISTED

    def test_matches_id_or_doi_case_insensitively(self):
        kept = keep_requested(self.LISTED, ["000402", "10.6080/k0ms3qnt"])
        assert [d["dataset_id"] for d in kept] == ["000402", "10.6080/K0MS3QNT"]

    def test_partial_match_keeps_what_was_found(self):
        assert len(keep_requested(self.LISTED, ["000070", "999999"])) == 1

    def test_nothing_found_fails_loudly(self):
        # A typo'd or vanished test dataset must fail the run, not ingest nothing and pass.
        with pytest.raises(ValueError, match="999999"):
            keep_requested(self.LISTED, ["999999"], archive="DANDI")

    def test_list_all_is_effectively_unbounded(self):
        assert LIST_ALL >= 100_000


class TestSqlIdList:
    def test_quotes_each_id(self):
        assert sql_id_list(["000402", "10.6080/k0ms3qnt", "ds004315", "hc-3"]) == \
            "'000402', '10.6080/k0ms3qnt', 'ds004315', 'hc-3'"

    @pytest.mark.parametrize("bad", ["x'; DROP TABLE papers; --", "a b", "50%", "", "x" * 200])
    def test_refuses_anything_that_could_break_out(self, bad):
        with pytest.raises(ValueError):
            sql_id_list([bad])

    def test_empty_list_is_refused(self):
        with pytest.raises(ValueError):
            sql_id_list([])


airflow = pytest.importorskip("airflow")


@pytest.mark.parametrize("module", [
    "dandi_ingestion", "openneuro_ingestion", "crcns_ingestion", "sparc_ingestion",
    "dandi_paper_mapping", "openneuro_paper_mapping", "crcns_paper_mapping", "sparc_paper_mapping",
    "paper_reuse_classification",
])
def test_dag_exposes_dataset_ids_defaulting_to_empty(module):
    mod = __import__(module)
    p = mod.dag.params["dataset_ids"] if hasattr(mod.dag.params, "__getitem__") else None
    assert p == [] or getattr(p, "value", None) == []


def test_requested_ids_reads_any_list_param():
    from utils.targeting import requested_ids
    assert requested_ids({"include_citing_dois": ["10.1186/X", ""]}, "include_citing_dois") == ["10.1186/X"]
    assert requested_ids({}, "include_citing_dois") == []
