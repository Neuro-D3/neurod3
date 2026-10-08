"""
Tests for utils/citing_paper_text.py: the citation step's concurrent full-text fetch.

No network and no database: ``fetch_fulltext_oa`` is replaced by a stub, the
cursor records what it is asked to run, and cache files go under ``tmp_path``.
"""

import json
import threading

import pytest

from utils import citing_paper_text as C
from utils.cache_keys import paper_cache_key_for_doi


class FakeCursor:
    """Answers the cache-key lookup from ``existing``; records every statement and the thread it ran on."""

    def __init__(self, existing=None):
        self.existing = dict(existing or {})
        self.statements = []
        self._rows = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params, threading.get_ident()))
        if sql.lstrip().startswith("SELECT paper_doi, fulltext_cache_key"):
            self._rows = [(d, self.existing[d]) for d in params[0] if d in self.existing]

    def fetchall(self):
        return self._rows

    def upserts(self):
        return [p for sql, p, _ in self.statements if "INSERT INTO papers" in sql]


class FakeConn:
    def __init__(self):
        self.commits = 0
        self.threads = set()

    def commit(self):
        self.commits += 1
        self.threads.add(threading.get_ident())


def citing(doi, **extra):
    return {"doi": doi, "openalex_id": f"W{doi[-1]}", "title": f"Paper {doi}", "authors": [],
            "publication_date": "2024-01-01", "publication_year": 2024, **extra}


def run(papers, *, cursor=None, params=None, tmp_path, fetch=None, monkeypatch):
    if fetch is not None:
        monkeypatch.setattr(C, "fetch_fulltext_oa", fetch)
    params = params or {}
    cursor = cursor or FakeCursor()
    conn = FakeConn()
    with C.citing_text_pool(params) as pool:
        out = list(C.ensure_citing_papers(conn, cursor, papers, params=params, output_root=tmp_path, pool=pool))
    return out, cursor, conn


def ok_fetch(session, doi, **kw):
    return f"body of {doi}", "europe_pmc", True, "ok"


class TestCachedPapers:
    def test_cached_papers_are_written_without_a_fetch(self, monkeypatch, tmp_path):
        def no_fetch(*a, **k):
            raise AssertionError("cached papers must not be fetched")
        cursor = FakeCursor({"10.1/a": "papers/x/latest.json"})
        out, cursor, conn = run([citing("10.1/a")], cursor=cursor, tmp_path=tmp_path,
                                fetch=no_fetch, monkeypatch=monkeypatch)
        assert out == [("10.1/a", {"paper_upserted": 1, "already_cached": 1,
                                   "fulltext_fetched": 0, "fulltext_unavailable": 0})]
        (row,) = cursor.upserts()
        assert row[0] == "10.1/a" and row[6:11] == (None, None, None, None, None)  # cache fields untouched (COALESCE)
        assert conn.commits == 1

    def test_force_refresh_fetches_cached_papers_again(self, monkeypatch, tmp_path):
        cursor = FakeCursor({"10.1/a": "papers/x/latest.json"})
        out, _, _ = run([citing("10.1/a")], cursor=cursor, params={"force_refresh_fulltext": True},
                        tmp_path=tmp_path, fetch=ok_fetch, monkeypatch=monkeypatch)
        assert out[0][1]["fulltext_fetched"] == 1


class TestFetchedPapers:
    def test_text_is_cached_on_disk_and_recorded_on_the_row(self, monkeypatch, tmp_path):
        out, cursor, _ = run([citing("10.1/a")], tmp_path=tmp_path, fetch=ok_fetch, monkeypatch=monkeypatch)
        assert out == [("10.1/a", {"paper_upserted": 1, "already_cached": 0,
                                   "fulltext_fetched": 1, "fulltext_unavailable": 0})]
        key = paper_cache_key_for_doi("10.1/a")
        payload = json.loads((tmp_path / key).read_text(encoding="utf-8"))
        assert payload["full_text"] == payload["text"] == "body of 10.1/a"
        assert payload["full_text_source"] == "europe_pmc" and payload["full_text_available"] is True
        (row,) = cursor.upserts()
        assert row[6] == key and row[8:11] == ("europe_pmc", True, "ok")
        assert row[11] == "openalex_citation"

    def test_no_text_is_still_cached_and_counted_unavailable(self, monkeypatch, tmp_path):
        def no_text(session, doi, **kw):
            return None, "none", False, "unavailable: no text"
        out, cursor, _ = run([citing("10.1/a")], tmp_path=tmp_path, fetch=no_text, monkeypatch=monkeypatch)
        assert out[0][1]["fulltext_unavailable"] == 1
        (row,) = cursor.upserts()
        assert row[6] == paper_cache_key_for_doi("10.1/a")   # a miss is remembered, as before
        assert row[9] is False

    def test_a_failing_fetch_is_not_cached_and_does_not_stop_the_others(self, monkeypatch, tmp_path):
        def flaky(session, doi, **kw):
            if doi == "10.1/b":
                raise RuntimeError("boom")
            return ok_fetch(session, doi)
        out, cursor, _ = run([citing("10.1/a"), citing("10.1/b"), citing("10.1/c")],
                             tmp_path=tmp_path, fetch=flaky, monkeypatch=monkeypatch)
        by_doi = dict(out)
        assert set(by_doi) == {"10.1/a", "10.1/b", "10.1/c"}
        assert by_doi["10.1/b"]["fulltext_unavailable"] == 1
        failed = [r for r in cursor.upserts() if r[0] == "10.1/b"][0]
        assert failed[6] is None                       # no cache key: the next run tries again
        assert not (tmp_path / paper_cache_key_for_doi("10.1/b")).exists()

    def test_a_fetcher_error_is_not_cached_so_the_next_run_retries(self, monkeypatch, tmp_path):
        # paper-text-fetcher raising is turned into an "unavailable: fetch_error"
        # result by the real fetch_fulltext_oa; it must not become a cached miss.
        from utils import paper_fulltext as P

        class RaisingFetcher:
            def get_paper_text_detailed(self, doi):
                raise TimeoutError("publisher timed out")

        monkeypatch.setattr(P, "get_paper_fetcher", lambda cache_dir=None: RaisingFetcher())
        out, cursor, _ = run([citing("10.1/a")], params={"min_api_interval_seconds": 0},
                             tmp_path=tmp_path, monkeypatch=monkeypatch)
        assert out[0][1]["fulltext_unavailable"] == 1
        assert cursor.upserts()[0][6] is None
        assert not (tmp_path / paper_cache_key_for_doi("10.1/a")).exists()

    def test_a_failing_cache_write_is_treated_like_a_failed_fetch(self, monkeypatch, tmp_path):
        blocker = tmp_path / "papers"
        blocker.write_text("not a directory")          # mkdir under it fails
        out, cursor, _ = run([citing("10.1/a")], tmp_path=tmp_path, fetch=ok_fetch, monkeypatch=monkeypatch)
        assert out[0][1]["fulltext_unavailable"] == 1
        assert cursor.upserts()[0][6] is None


class TestConcurrency:
    def test_fetches_overlap_and_every_database_write_stays_on_the_caller(self, monkeypatch, tmp_path):
        # Four fetches must all be in flight at once to get past the barrier;
        # run serially, the first would time out waiting for the others.
        barrier = threading.Barrier(4, timeout=5)
        fetch_threads = set()

        def together(session, doi, **kw):
            fetch_threads.add(threading.get_ident())
            barrier.wait()
            return ok_fetch(session, doi)

        papers = [citing(f"10.1/{c}") for c in "abcd"]
        out, cursor, conn = run(papers, params={"fulltext_workers": 4}, tmp_path=tmp_path,
                                fetch=together, monkeypatch=monkeypatch)
        assert sorted(d for d, _ in out) == ["10.1/a", "10.1/b", "10.1/c", "10.1/d"]
        assert len(fetch_threads) == 4
        caller = threading.get_ident()
        assert {t for _, _, t in cursor.statements} == {caller}
        assert conn.threads == {caller}
        assert caller not in fetch_threads

    def test_one_worker_fetches_one_paper_at_a_time(self, monkeypatch, tmp_path):
        state = {"now": 0, "max": 0}
        lock = threading.Lock()

        def counted(session, doi, **kw):
            with lock:
                state["now"] += 1
                state["max"] = max(state["max"], state["now"])
            with lock:
                state["now"] -= 1
            return ok_fetch(session, doi)

        out, _, _ = run([citing(f"10.1/{c}") for c in "abcde"], params={"fulltext_workers": 1},
                        tmp_path=tmp_path, fetch=counted, monkeypatch=monkeypatch)
        assert len(out) == 5 and state["max"] == 1


class TestInputs:
    def test_duplicate_and_invalid_dois_are_skipped(self, monkeypatch, tmp_path):
        calls = []

        def recorded(session, doi, **kw):
            calls.append(doi)
            return ok_fetch(session, doi)

        # The same DOI written two ways, and two that are not DOIs at all.
        papers = [citing("10.1/a"), citing("https://doi.org/10.1/a"), {"doi": None}, {"doi": "   "}]
        out, _, _ = run(papers, tmp_path=tmp_path, fetch=recorded, monkeypatch=monkeypatch)
        assert [d for d, _ in out] == ["10.1/a"] and calls == ["10.1/a"]

    def test_no_papers_runs_no_query(self, monkeypatch, tmp_path):
        out, cursor, conn = run([], tmp_path=tmp_path, fetch=ok_fetch, monkeypatch=monkeypatch)
        assert out == [] and cursor.statements == [] and conn.commits == 0

    @pytest.mark.parametrize("value, expected", [(None, 8), ("4", 4), (0, 1), (-3, 1), (500, 32), ("x", 8)])
    def test_worker_count_is_clamped(self, value, expected):
        params = {} if value is None else {"fulltext_workers": value}
        assert C.fulltext_workers(params) == expected


def test_stopping_early_cancels_downloads_that_have_not_started(monkeypatch, tmp_path):
    started = []
    gate = threading.Event()

    def slow(session, doi, **kw):
        started.append(doi)
        gate.wait(5)
        return ok_fetch(session, doi)

    monkeypatch.setattr(C, "fetch_fulltext_oa", slow)
    papers = [citing(f"10.1/{c}") for c in "abcdefgh"]
    params = {"fulltext_workers": 2}
    with C.citing_text_pool(params) as pool:
        gen = C.ensure_citing_papers(FakeConn(), FakeCursor(), papers, params=params, output_root=tmp_path, pool=pool)
        threading.Timer(0.2, gate.set).start()
        next(gen)          # first download finishes once the gate opens
        gen.close()        # the caller gives up: queued downloads are cancelled
    assert len(started) < len(papers)
