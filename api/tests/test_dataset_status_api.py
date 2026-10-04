"""
Tests for the API's use of dataset_status (utils/dataset_status.py in the DAGs):
junk datasets are left out of the dataset list and stats, and the paper-mapping
summary carries each archive's funnel. No database: a fake dict-row cursor
answers the schema lookups and the funnel queries.
"""

import main as M


class FakeCursor:
    def __init__(self, tables=(), columns=None, funnel_row=None, reasons=()):
        self.tables = set(tables)
        self.columns = dict(columns or {})
        self.funnel_row = funnel_row
        self.reasons = list(reasons)
        self.queries = []
        self._rows = []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.queries.append((s, params))
        params = params or ()
        if "information_schema.tables" in s:
            self._rows = [{"exists": params[0] in self.tables}]
        elif "information_schema.columns" in s:
            self._rows = [{"exists": params[1] in self.columns.get(params[0], set())}]
        elif "dataset_status_reason" in s:
            self._rows = [{"reason": r, "n": n} for r, n in self.reasons]
        elif "AS ingested_total" in s:
            self._rows = [dict(self.funnel_row)]
        else:
            self._rows = []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class TestVisibleSql:
    def test_filters_only_when_the_column_exists(self):
        assert M._dataset_visible_sql("d", False) == ""
        assert M._dataset_visible_sql("d", True) == " AND COALESCE(d.dataset_status, '') <> 'excluded'"
        assert M._dataset_visible_sql("", True) == " AND COALESCE(dataset_status, '') <> 'excluded'"


class TestDatasetFunnel:
    def test_old_schema_gives_nulls(self):
        cur = FakeCursor(tables={"dandi_dataset"}, columns={"dandi_dataset": {"title"}})
        out = M._dataset_funnel(cur, "DANDI")
        assert out == {k: None for k in M._DATASET_FUNNEL_KEYS}
        cur = FakeCursor(tables=set())
        assert M._dataset_funnel(cur, "SPARC")["ingested_total"] is None

    def test_unknown_source(self):
        assert M._dataset_funnel(FakeCursor(), "Kaggle")["junk_datasets"] is None

    def test_dandi_counts_draft_only_dandisets(self):
        cur = FakeCursor(
            tables={"dandi_dataset"},
            columns={"dandi_dataset": {"dataset_status", "version"}},
            funnel_row={
                "ingested_total": 920, "junk_datasets": 80, "ingested_datasets": 840,
                "no_paper_datasets": 500, "pending_datasets": 75, "never_published_datasets": 410,
            },
            reasons=[("find_reuse_test_id", 59), ("placeholder_title", 21)],
        )
        out = M._dataset_funnel(cur, "DANDI")
        assert out["ingested_datasets"] == 840
        assert out["never_published_datasets"] == 410
        assert out["junk_reasons"] == {"find_reuse_test_id": 59, "placeholder_title": 21}
        funnel_sql = next(s for s, _ in cur.queries if "AS ingested_total" in s)
        assert "FROM dandi_dataset" in funnel_sql
        assert "version = 'draft'" in funnel_sql
        assert "dataset_status = 'excluded'" in funnel_sql

    def test_other_archives_skip_never_published(self):
        cur = FakeCursor(
            tables={"sparc_dataset"},
            columns={"sparc_dataset": {"dataset_status", "version"}},
            funnel_row={
                "ingested_total": 420, "junk_datasets": 9, "ingested_datasets": 411,
                "no_paper_datasets": 237, "pending_datasets": 0, "never_published_datasets": None,
            },
        )
        out = M._dataset_funnel(cur, "SPARC")
        assert out["never_published_datasets"] is None
        assert out["junk_reasons"] == {}
        funnel_sql = next(s for s, _ in cur.queries if "AS ingested_total" in s)
        assert "NULL::int AS never_published_datasets" in funnel_sql
