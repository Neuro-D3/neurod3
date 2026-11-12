"""utils/citation_status.py: per (dataset, primary paper) record of whether citing papers were fetched."""

import pytest

from utils import citation_status as S


class FakeCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))


def test_ddl_adds_four_columns():
    ddl = S.citation_status_ddl("dandi_paper_map")
    for col in ("citations_status TEXT", "citations_checked_at TIMESTAMPTZ", "citations_error TEXT",
                "citations_attempts INTEGER"):
        assert f"ALTER TABLE dandi_paper_map ADD COLUMN IF NOT EXISTS {col};" in ddl


def test_mark_failed_keeps_the_error():
    cur = FakeCursor()
    S.mark_citations(cur, "dandi_paper_map", "dandi_id", "000034", "10.7554/elife.61834", S.STATUS_FAILED, "boom")
    sql, params = cur.executed[0]
    assert sql.startswith("UPDATE dandi_paper_map SET citations_status = %s")
    assert "WHERE dandi_id = %s AND paper_doi = %s" in sql
    assert params == ("failed", "boom", "000034", "10.7554/elife.61834")
    # Each failure in a row counts towards the retry limit.
    assert "citations_attempts = COALESCE(citations_attempts, 0) + 1" in sql


def test_transient_failure_is_marked_but_not_counted():
    # An outage (5xx, timeout) must not use up the retry limit.
    cur = FakeCursor()
    S.mark_citations(cur, "dandi_paper_map", "dandi_id", "000034", "10.1/x", S.STATUS_FAILED, "HTTP 503",
                     count_attempt=False)
    sql, params = cur.executed[0]
    assert params == ("failed", "HTTP 503", "000034", "10.1/x")
    assert "citations_attempts = COALESCE(citations_attempts, 0) WHERE" in sql


def test_mark_complete_clears_the_error():
    cur = FakeCursor()
    S.mark_citations(cur, "sparc_paper_map", "sparc_id", "157", "10.1/x", S.STATUS_COMPLETE, "ignored")
    sql, params = cur.executed[0]
    assert params == ("complete", None, "157", "10.1/x")
    # A success starts the count again.
    assert "citations_attempts = 0" in sql


def test_long_errors_are_cut():
    cur = FakeCursor()
    S.mark_citations(cur, "crcns_paper_map", "crcns_id", "hc-3", "10.1/x", S.STATUS_FAILED, "x" * 5000)
    assert len(cur.executed[0][1][1]) == 1000


def test_mark_none_clears_the_status():
    # Nothing to fetch (no OpenAlex id): an earlier 'failed' must not stay and be retried.
    cur = FakeCursor()
    S.mark_citations(cur, "dandi_paper_map", "dandi_id", "000034", "10.1/x", None)
    sql, params = cur.executed[0]
    assert params == (None, None, "000034", "10.1/x")
    assert "citations_attempts = 0" in sql


def test_mark_truncated():
    cur = FakeCursor()
    S.mark_citations(cur, "dandi_paper_map", "dandi_id", "000034", "10.1/x", S.STATUS_TRUNCATED)
    assert cur.executed[0][1] == ("truncated", None, "000034", "10.1/x")


def test_status_of_a_fetch_that_raised_nothing():
    assert S.fetched_citations_status(0, False) is None
    assert S.fetched_citations_status(1, False) == "complete"
    assert S.fetched_citations_status(2, True) == "truncated"


def test_unknown_status_is_refused():
    with pytest.raises(ValueError):
        S.mark_citations(FakeCursor(), "dandi_paper_map", "dandi_id", "1", "d", "partial")


def test_failed_condition_sql_stops_at_the_retry_limit():
    assert S.MAX_CITATION_ATTEMPTS == 3
    assert S.has_failed_citations_sql("openneuro_paper_map", "openneuro_id") == (
        "EXISTS (SELECT 1 FROM openneuro_paper_map cf "
        "WHERE cf.openneuro_id = d.dataset_id AND cf.citations_status = 'failed' "
        "AND COALESCE(cf.citations_attempts, 0) < 3)"
    )


def test_past_retry_limit_count_sql():
    assert S.count_past_retry_limit_sql("crcns_paper_map") == (
        "SELECT COUNT(*) FROM crcns_paper_map "
        "WHERE citations_status = 'failed' AND COALESCE(citations_attempts, 0) >= 3"
    )
