"""
Tests for the SQL the API builds around paper reuse classifications.

No database: a fake dict-row cursor answers the information_schema queries
with a configurable set of tables and columns, and the tests assert on the
SQL text that comes out.
"""

import re


import main as M


class FakeCursor:
    """Answers the two information_schema lookups the helpers make."""

    def __init__(self, tables=None, columns=None):
        self.tables = set(tables or [])
        self.columns = dict(columns or {})   # table -> set(columns)
        self._rows = []
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((" ".join(sql.split()), params))
        params = params or ()
        if "information_schema.tables" in sql:
            self._rows = [{"exists": params[0] in self.tables}]
        elif "information_schema.views" in sql:
            self._rows = [{"exists": False}]
        elif "information_schema.columns" in sql:
            self._rows = [{"column_name": c} for c in sorted(self.columns.get(params[0], set()))]
        else:
            self._rows = []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


ALL_EXTRA = {c for c, _t in M.CLASSIFICATION_EXTRA_COLUMNS}


class TestClassificationExtraColumns:
    def test_migrated_table_selects_the_real_columns(self):
        cur = FakeCursor(columns={"dandi_paper_citation_classifications": ALL_EXTRA | {"classification"}})
        frag = M._classification_extra_columns_sql(cur, "dandi_paper_citation_classifications")
        for column in ALL_EXTRA:
            assert f"c.{column}" in frag
        assert "NULL::" not in frag

    def test_unmigrated_table_selects_typed_nulls(self):
        cur = FakeCursor(columns={"sparc_paper_citation_classifications": {"classification", "status"}})
        frag = M._classification_extra_columns_sql(cur, "sparc_paper_citation_classifications")
        assert "NULL::text AS reuse_type" in frag
        assert "NULL::jsonb AS evidence_quotes" in frag
        assert "NULL::integer AS prompt_version" in frag
        assert "c.reuse_type" not in frag

    def test_partial_migration_mixes_both(self):
        cur = FakeCursor(columns={"crcns_paper_citation_classifications": {"reuse_type"}})
        frag = M._classification_extra_columns_sql(cur, "crcns_paper_citation_classifications")
        assert "c.reuse_type" in frag
        assert "NULL::jsonb AS reused_modalities" in frag

    def test_no_cursor_means_all_nulls(self):
        frag = M._classification_extra_columns_sql(None, "dandi_paper_citation_classifications")
        assert frag.count("NULL::") == len(M.CLASSIFICATION_EXTRA_COLUMNS)

    def test_union_branches_stay_column_aligned(self):
        """Every classification branch must list the extras in the same order."""
        cur = FakeCursor(
            tables={"crcns_dataset", "crcns_paper_map", "crcns_paper_citations",
                    "crcns_paper_citation_classifications", "sparc_dataset", "sparc_paper_map",
                    "sparc_paper_citations", "sparc_paper_citation_classifications"},
            columns={"dandi_paper_citation_classifications": ALL_EXTRA,
                     "openneuro_paper_citation_classifications": set(),
                     "crcns_paper_citation_classifications": ALL_EXTRA,
                     "sparc_paper_citation_classifications": {"reuse_type"}},
        )
        ctes = M._paper_mapping_ctes(cur)
        branches = re.findall(r"SELECT\s+'(\w+)'::text AS source,.*?FROM (\w+_paper_citation_classifications) c", ctes, re.S)
        assert [b[0] for b in branches] == ["DANDI", "OpenNeuro", "CRCNS", "SPARC"]
        for _source, table in branches:
            branch = ctes.split(f"FROM {table} c")[0].rsplit("SELECT", 1)[1]
            names = [
                (m.group(2) or m.group(1))
                for m in re.finditer(r"(?:c\.(\w+)|NULL::\w+ AS (\w+))", branch)
                if (m.group(2) or m.group(1)) in ALL_EXTRA
            ]
            assert names == [c for c, _t in M.CLASSIFICATION_EXTRA_COLUMNS], table


class TestReuseCountSubquery:
    def test_counts_reuse_for_every_present_source(self):
        cur = FakeCursor(tables={t for t, _ in M._CLASSIFICATION_TABLES})
        sql = M._reuse_count_subquery(cur)
        assert sql.count("COUNT(DISTINCT citing_paper_doi)") == 4
        assert "IN ('REUSE')" in sql
        for _table, id_col in M._CLASSIFICATION_TABLES:
            assert f"{id_col} = d.dataset_id" in sql
        assert "SECONDARY" not in sql  # the retired label is gone for good

    def test_only_existing_tables_are_summed(self):
        cur = FakeCursor(tables={"dandi_paper_citation_classifications", "sparc_paper_citation_classifications"})
        sql = M._reuse_count_subquery(cur)
        assert "dandi_paper_citation_classifications" in sql
        assert "sparc_paper_citation_classifications" in sql
        assert "openneuro_paper_citation_classifications" not in sql
        assert sql.count(" + ") == 1

    def test_no_tables_is_zero(self):
        assert M._reuse_count_subquery(FakeCursor()) == "0"

    def test_reuse_labels_constant(self):
        assert M.REUSE_CLASSIFICATIONS == ("REUSE",)
