"""
Tests for the ORDER BY the dataset list builds (GET /api/datasets).

The site opens on sort_by=reuse, and most datasets have no reuse yet, so how
ties are broken decides what most of the page shows.
"""

import pytest
from fastapi import HTTPException

import main as M

REUSE = "SELECT reuse"


class TestDatasetOrderSql:
    def test_reuse_ties_stay_newest_first(self):
        assert M._dataset_order_sql("reuse", "desc", REUSE) == (
            f"({REUSE}) DESC NULLS LAST, d.created_at DESC NULLS LAST, d.title ASC, d.dataset_id ASC"
        )

    def test_other_sorts_break_ties_by_title(self):
        assert M._dataset_order_sql("title", "asc", REUSE) == "d.title ASC NULLS LAST, d.title ASC, d.dataset_id ASC"

    def test_defaults_to_newest_first(self):
        assert M._dataset_order_sql(None, None, REUSE) == (
            "d.created_at DESC NULLS LAST, d.title ASC, d.dataset_id ASC"
        )

    def test_papers_counts_reuse_too(self):
        assert M._dataset_order_sql("papers", "desc", REUSE).startswith(
            f"(COALESCE(d.papers, 0) + ({REUSE})) DESC"
        )

    @pytest.mark.parametrize("sort_by, sort_order", [("downloads", "desc"), ("reuse", "sideways")])
    def test_unknown_sort_is_a_400(self, sort_by, sort_order):
        with pytest.raises(HTTPException) as err:
            M._dataset_order_sql(sort_by, sort_order, REUSE)
        assert err.value.status_code == 400
