"""
Tests for the pure helpers in the paper_reuse_classification DAG.

Importing the DAG module needs Airflow (present in the containers, where the
suite runs). No database and no network: the helpers under test take plain
dicts and return SQL fragments or lists.
"""

import pytest

pytest.importorskip("airflow")

import paper_reuse_classification as D  # noqa: E402
from utils import classify_fulltext_reuse as C  # noqa: E402


def edge(source="dandi", dataset="000001", primary="10.1/p", citing="10.1/c", edge_type="citation_edge"):
    return {"source": source, "dataset_id": dataset, "primary_paper_doi": primary,
            "citing_paper_doi": citing, "edge_type": edge_type}


class TestModeAndParams:
    def test_scope_maps_to_upstream_modes(self):
        assert D._mode_for_edge_type("citation_edge") == C.MODE_CITING
        assert D._mode_for_edge_type("primary") == C.MODE_DIRECT

    def test_model_defaults_to_the_classifiers_model_not_llm_classify(self):
        assert D._resolve_model(None) == C.DEFAULT_MODEL
        assert D._resolve_model("  ") == C.DEFAULT_MODEL
        assert D._resolve_model("openai/gpt-5.4-nano") == "openai/gpt-5.4-nano"

    @pytest.mark.parametrize("raw,expected", [
        ("max", "max"), ("LOW", "low"), ("none", None), ("", None), (None, None),
    ])
    def test_reasoning_effort_normalizes(self, raw, expected):
        assert D._resolve_reasoning_effort(raw) == expected

    def test_unknown_reasoning_effort_fails_before_any_call(self):
        with pytest.raises(D.AirflowFailException):
            D._resolve_reasoning_effort("extreme")

    def test_source_filter_aliases(self):
        assert D._normalize_repository_filter("dandi") == "DANDI"
        assert D._normalize_repository_filter("OpenNeuro") == "OpenNeuro"
        assert D._normalize_repository_filter(None) == "all"
        assert D._normalize_repository_filter("nonsense") == "all"

    def test_only_sources_with_citation_tables_are_selected(self):
        assert [r[0] for r in D._citation_table_sources_for_filter("all")] == ["dandi", "openneuro", "crcns", "sparc"]
        assert [r[0] for r in D._citation_table_sources_for_filter("CRCNS")] == ["crcns"]
        assert D._citation_table_sources_for_filter("Kaggle") == []


class TestCandidatePredicate:
    def test_reclassify_selects_everything(self):
        sql, params = D._candidate_status_filter(True, 6, "m")
        assert sql == "" and params == {}

    def test_predicate_covers_new_failed_and_stale_rows(self):
        sql, params = D._candidate_status_filter(False, C.PROMPT_VERSION, "openai/gpt-5.6-luna")
        assert "cls.id IS NULL" in sql
        for status in ("error", "placeholder", "no_full_text", "dry_run"):
            assert f"'{status}'" in sql
        assert "cls.prompt_version IS DISTINCT FROM %(prompt_version)s" in sql
        assert "cls.classification_model IS DISTINCT FROM %(model)s" in sql
        assert params == {"prompt_version": C.PROMPT_VERSION, "model": "openai/gpt-5.6-luna"}

    def test_a_good_row_is_not_reselected_by_status(self):
        sql, _ = D._candidate_status_filter(False, 6, "m")
        assert "'classified'" not in sql


class TestCandidateOrder:
    def test_default_is_citing_doi_order(self):
        sql = D._citation_edge_order_sql(False)
        assert sql.startswith("CASE WHEN cls.id IS NULL THEN 0")
        assert sql.endswith("cit.citing_paper_doi, cit.resolved_at DESC")
        assert "ROW_NUMBER" not in sql

    def test_mix_publishers_round_robins_doi_prefixes(self):
        sql = D._citation_edge_order_sql(True)
        # Unclassified rows still come first; within a priority, deal one pair
        # per DOI prefix at a time in a stable hash order.
        assert sql.startswith("CASE WHEN cls.id IS NULL THEN 0")
        assert "ROW_NUMBER() OVER (PARTITION BY" in sql
        assert "split_part(cit.citing_paper_doi, '/', 1)" in sql
        assert "md5(" in sql and "random()" not in sql

    def test_param_defaults_off(self):
        p = D._build_dag_params()["mix_publishers"]
        assert getattr(p, "value", p) is False


class TestBatching:
    def test_pairs_of_one_paper_form_one_group_in_first_seen_order(self):
        edges = [edge(dataset="1", citing="10.1/a"), edge(dataset="2", citing="10.1/b"),
                 edge(dataset="3", citing="10.1/a"), edge(dataset="4", citing="10.1/c")]
        groups = D._group_edges_by_paper(edges)
        assert [[e["dataset_id"] for e in g] for g in groups] == [["1", "3"], ["2"], ["4"]]

    def test_same_doi_in_different_sources_stays_apart(self):
        groups = D._group_edges_by_paper([edge(source="dandi"), edge(source="openneuro")])
        assert len(groups) == 2

    def test_batches_hold_whole_papers_only(self):
        groups = [[edge(dataset=str(i), citing=f"10.1/{i}")] * (i + 1) for i in range(5)]
        batches = D._pack_paper_groups(groups, 2)
        assert [len(b) for b in batches] == [2, 2, 1]
        assert batches[0][1][0]["citing_paper_doi"] == "10.1/1"
        # no group is split across batches
        seen = [g[0]["citing_paper_doi"] for b in batches for g in b]
        assert seen == [f"10.1/{i}" for i in range(5)]

    def test_batch_size_below_one_is_clamped(self):
        assert len(D._pack_paper_groups([[edge()], [edge()]], 0)) == 2

    def test_empty_input(self):
        assert D._group_edges_by_paper([]) == []
        assert D._pack_paper_groups([], 10) == []


class TestResultRow:
    def test_success_result_maps_every_new_column(self):
        result = {
            "classification": "REUSE", "confidence": 8, "reasoning": "r", "mode": "citing",
            "prompt_version": 6, "reuse_type": "BENCHMARK", "reuse_type_other": None,
            "reused_modalities": ["neurophysiology"], "reused_dandi_hosted": True,
            "reused_neurophysiology": True, "same_lab": False, "same_lab_confidence": 9,
            "source_archive": "DANDI Archive",
            "evidence_quotes": [{"quote": "we downloaded", "match_type": "exact", "verbatim": True}],
            "source_quotes": [], "quote_warnings": [], "hallucinated_quote_count": 0,
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "cost": 0.01},
            "input_chars": 12345, "truncation": None, "provider": "OpenAI", "error_kind": None,
        }
        row = D._result_to_row(result, D.STATUS_CLASSIFIED, "m", "citing")
        assert row["classification"] == "REUSE"
        assert row["status"] == "classified"
        assert row["classification_model"] == "m"
        assert row["reuse_type"] == "BENCHMARK"
        assert row["reused_modalities"] == ["neurophysiology"]
        assert row["evidence_quotes"][0]["match_type"] == "exact"
        assert row["usage"]["cost"] == 0.01
        assert row["provider"] == "OpenAI"
        assert row["prompt_version"] == 6

    def test_error_result_has_no_label_but_keeps_the_reason(self):
        result = C._error_result("network_error", "timed out")
        row = D._result_to_row(result, D.STATUS_ERROR, "m", "direct")
        assert row["classification"] is None
        assert row["status"] == "error"
        assert row["error_kind"] == "network_error"
        assert row["reasoning"] == "timed out"
        assert row["mode"] == "direct"
        assert row["reused_modalities"] == [] and row["evidence_quotes"] == []

    def test_usage_numbers(self):
        assert D._usage_numbers(None) == (0, 0, 0, None)
        assert D._usage_numbers({"prompt_tokens": 3, "completion_tokens": 2}) == (3, 2, 5, None)
        assert D._usage_numbers({"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 9, "cost": "0.5"}) == (3, 2, 9, 0.5)


class TestUpsertGuard:
    """The SQL text is asserted because the guard lives in the statement, not in Python."""

    class RecordingCursor:
        def __init__(self):
            self.sql = None
            self.values = None
            self.rowcount = 1

        def execute(self, sql, values=None):
            self.sql, self.values = sql, values

    def test_non_results_never_replace_a_real_judgement(self):
        cur = self.RecordingCursor()
        row = D._result_to_row(C._error_result("x", "y"), D.STATUS_ERROR, "m", "citing")
        D._upsert_classification(cur, "dandi", "000001", "10.1/p", "10.1/c", row, "run")
        assert "ON CONFLICT (dandi_id, primary_paper_doi, citing_paper_doi) DO UPDATE" in cur.sql
        assert "EXCLUDED.status IN ('error', 'no_full_text', 'dry_run')" in cur.sql
        assert "dandi_paper_citation_classifications.status NOT IN ('error', 'placeholder', 'no_full_text', 'dry_run')" in cur.sql
        assert cur.values["status"] == "error"
        assert cur.values["classification"] is None

    def test_unknown_source_writes_nothing(self):
        cur = self.RecordingCursor()
        assert D._upsert_classification(cur, "kaggle", "1", "p", "c", {}, "run") == 0
        assert cur.sql is None

    @pytest.mark.parametrize("source,table,id_col", [
        ("openneuro", "openneuro_paper_citation_classifications", "openneuro_id"),
        ("crcns", "crcns_paper_citation_classifications", "crcns_id"),
        ("sparc", "sparc_paper_citation_classifications", "sparc_id"),
    ])
    def test_each_source_targets_its_own_table(self, source, table, id_col):
        cur = self.RecordingCursor()
        row = D._result_to_row({"classification": "MENTION", "confidence": 5}, D.STATUS_CLASSIFIED, "m", "citing")
        D._upsert_classification(cur, source, "1", "p", "c", row, "run")
        assert f"INSERT INTO {table} (" in cur.sql
        assert f"ON CONFLICT ({id_col}, primary_paper_doi, citing_paper_doi)" in cur.sql


class TestMergeStats:
    def test_batches_are_summed(self):
        a = D._new_batch_stats(); a.update(pairs=2, classified=2, by_category={"REUSE": 1, "MENTION": 1})
        a["usage"].update(prompt_tokens=10, cost_usd=0.5, cost_known_for=2)
        b = D._new_batch_stats(); b.update(pairs=1, errors=1, hallucinated_quotes=3, quote_tiers={"exact": 4})
        total = D._merge_stats([{"stats": a}, {"stats": b}, None, {"nope": 1}])
        assert total["batches"] == 2
        assert total["pairs"] == 3 and total["classified"] == 2 and total["errors"] == 1
        assert total["by_category"] == {"REUSE": 1, "MENTION": 1}
        assert total["quote_tiers"] == {"exact": 4}
        assert total["hallucinated_quotes"] == 3
        assert total["usage"]["prompt_tokens"] == 10
        assert total["usage"]["cost_usd"] == 0.5 and total["usage"]["cost_known_for"] == 2


class TestFailedUpstreams:
    """The summary task runs on all_done; it must still fail the run when upstream work failed."""

    def test_nothing_failed(self):
        assert D._failed_upstreams([], [], []) == []
        assert D._failed_upstreams([{"e": 1}], [{"b": 1}, {"b": 2}], [{"stats": {}}, {"stats": {}}]) == []

    def test_fetch_failure_leaves_no_xcom(self):
        assert D._failed_upstreams(None, None, []) == ["fetch_unclassified_edges"]

    def test_batch_builder_failure(self):
        assert D._failed_upstreams([{"e": 1}], None, []) == ["build_classification_batches"]

    def test_missing_mapped_results_are_counted(self):
        out = D._failed_upstreams([{"e": 1}], [{"b": 1}, {"b": 2}, {"b": 3}], [{"stats": {}}])
        assert out == ["classify_and_persist_batch (2 of 3 batches)"]

    def test_no_edges_is_not_a_failure(self):
        # fetch ran and found nothing: empty list, and no batches to run
        assert D._failed_upstreams([], [], []) == []


class TestDagShape:
    def test_task_graph(self):
        ids = set(D.dag.task_ids)
        assert ids == {"create_classification_schema", "fetch_unclassified_edges",
                       "build_classification_batches", "classify_and_persist_batch",
                       "summarize_classification_run"}
        assert D.dag.get_task("classify_and_persist_batch").pool == D.POOL_NAME
        assert D.dag.get_task("fetch_unclassified_edges").upstream_task_ids == {"create_classification_schema"}
        assert D.dag.get_task("summarize_classification_run").trigger_rule == "all_done"

    def test_default_params_follow_upstream(self):
        p = {k: (v.value if hasattr(v, "value") else v) for k, v in D.dag.params.items()}
        assert p["model"] == C.DEFAULT_MODEL
        assert p["reasoning_effort"] == "max"
        assert p["temperature"] == 0.0
        assert p["max_tokens"] == C.DEFAULT_MAX_TOKENS
        assert p["max_input_chars"] == C.DEFAULT_MAX_INPUT_CHARS
        assert p["fetch_missing_fulltext"] is True
        assert p["dry_run"] is False


class TestFirstDois:
    def test_known_pairs_are_ordered_before_everything(self):
        sql = D._citation_edge_order_sql(False, "'10.1186/s12987-023-00425-4'")
        assert sql.startswith("CASE WHEN lower(cit.citing_paper_doi) IN ('10.1186/s12987-023-00425-4') THEN 0 ELSE 1 END, ")
        # The normal order still follows.
        assert "CASE WHEN cls.id IS NULL THEN 0" in sql

    def test_also_with_mixed_publishers(self):
        assert D._citation_edge_order_sql(True, "'10.1/x'").startswith("CASE WHEN lower(cit.citing_paper_doi) IN ('10.1/x')")

    def test_no_first_dois_leaves_the_order_unchanged(self):
        assert D._citation_edge_order_sql(False) == D._citation_edge_order_sql(False, "")
        assert not D._citation_edge_order_sql(False).startswith("CASE WHEN lower(")

    def test_param_defaults_empty(self):
        p = D._build_dag_params()["include_citing_dois"]
        assert getattr(p, "value", p) == []
