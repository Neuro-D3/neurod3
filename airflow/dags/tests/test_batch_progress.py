"""
Tests for utils/batch_progress.py and the progress lines the four paper-mapping
DAGs' citation and context batches log. No database, no network.
"""

import importlib
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from utils.batch_progress import BatchProgress, format_elapsed
from utils.find_reuse_core import Telemetry


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def make(clock, **kw):
    log = logging.getLogger("test.batch_progress")
    return BatchProgress("Citations batch 3", 4, "primary papers", clock=clock, log=log, **kw)


class TestBatchProgress:
    def test_elapsed_format(self):
        assert format_elapsed(0) == "0m00s"
        assert format_elapsed(725) == "12m05s"
        assert format_elapsed(3725) == "1h02m05s"

    def test_line_shows_progress_counters_openalex_and_note(self):
        clock = FakeClock()
        tel = Telemetry(total_requests=120, api_429_count=2, api_retry_count=3)
        metrics = {"citation_edges_upserted": 0}
        p = make(clock, counters=metrics, telemetry=tel)
        metrics["citation_edges_upserted"] = 57  # counters are read live, not copied
        clock.t += 125
        p.done = 1
        line = p.line("alm-3 10.1/x")
        assert line.startswith("Citations batch 3: 1/4 primary papers (25%)")
        assert "citation_edges_upserted=57" in line
        assert "OpenAlex requests=120 429s=2 retries=3" in line
        assert "2m05s elapsed" in line
        assert line.endswith("alm-3 10.1/x")

    def test_heartbeat_is_throttled_but_forced_lines_are_not(self, caplog):
        clock = FakeClock()
        p = make(clock, every_seconds=60)
        with caplog.at_level(logging.INFO, logger="test.batch_progress"):
            assert p.update(0, "start", force=True)
            assert not p.update(note="citing paper 1/900")  # too soon
            clock.t += 59
            assert not p.update(note="citing paper 400/900")
            clock.t += 2
            assert p.update(note="citing paper 800/900")  # 61s since the last line
            assert p.update(1, "next paper", force=True)
        assert len(caplog.records) == 3

    def test_empty_batch_has_no_percentage(self):
        p = BatchProgress("Contexts batch 0", 0, "citation edges", clock=FakeClock())
        assert p.line().startswith("Contexts batch 0: 0/0 citation edges |")


# ---------------------------------------------------------------------------
# The DAGs' batch functions, run against a fake database and API
# ---------------------------------------------------------------------------

ARCHIVES = ["crcns", "dandi", "openneuro", "sparc"]


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, *a, **k):
        pass

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return FakeCursor(self._rows)

    def commit(self):
        pass


def fake_db(rows):
    @contextmanager
    def conn():
        yield FakeConn(rows)
    return conn


@pytest.fixture(params=ARCHIVES)
def dag_module(request, monkeypatch):
    pytest.importorskip("airflow")
    mod = importlib.import_module(f"{request.param}_paper_mapping")
    monkeypatch.setattr(mod, "fetch_openalex_budget", lambda session=None: {"remaining": 9000, "limit": 10000})
    return request.param, mod


def test_citation_batch_logs_each_primary_paper_and_the_finish(dag_module, monkeypatch, caplog):
    archive, mod = dag_module
    created = datetime(2020, 1, 1, tzinfo=timezone.utc)
    # dataset_id, created_at, paper_doi, openalex_id, title, authors, publication_date, publication_year
    rows = [("ds1", created, "10.1/a", "W1", "A", [], "2019-01-01", 2019),
            ("ds1", created, "10.1/b", "W2", "B", [], "2019-01-01", 2019)]
    monkeypatch.setattr(mod, "get_db_connection", fake_db(rows))
    monkeypatch.setattr(mod, "get_alternate_doi", lambda *a, **k: None)
    monkeypatch.setattr(mod, "get_citing_papers",
                        lambda *a, **k: [{"doi": "10.9/c1"}, {"doi": "10.9/c2"}, {"doi": "10.9/c3"}])
    monkeypatch.setattr(mod, "_ensure_citing_paper_record", lambda **k: {"paper_upserted": True})

    with caplog.at_level(logging.INFO):
        out = mod.fetch_and_persist_citations_batch(batch_index=7, dataset_ids=["ds1"], run_id="r",
                                                    params={"max_citing_papers_per_primary": 10})

    assert out["citation_edges_upserted"] == 6
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "Citations batch 7: 1 datasets, 2 primary papers. OpenAlex budget: 9,000 of 10,000" in text
    assert "Citations batch 7: 0/2 primary papers (0%)" in text and "starting ds1 10.1/a" in text
    assert "ds1 10.1/b: 3 citing papers from OpenAlex" in text
    assert "Citations batch 7: 2/2 primary papers (100%)" in text and "batch finished" in text


def test_context_batch_logs_start_and_finish(dag_module, monkeypatch, caplog):
    archive, mod = dag_module
    # id, primary_doi, citing_doi, contexts_extracted_at, primary title, authors, year, fulltext key
    rows = [("ds1", "10.1/a", "10.9/c1", None, "A", [], 2019, None),
            ("ds1", "10.1/a", "10.9/c2", None, "A", [], 2019, None)]
    monkeypatch.setattr(mod, "get_db_connection", fake_db(rows))
    monkeypatch.setattr(mod, "_load_cached_text", lambda key: None)

    with caplog.at_level(logging.INFO):
        out = mod.extract_and_persist_citation_contexts_batch(batch_index=2, dataset_ids=["ds1"], run_id="r",
                                                              params={})

    assert out["citation_edges_updated"] == 2
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "Contexts batch 2: 0/2 citation edges (0%)" in text and "starting" in text
    assert "Contexts batch 2: 2/2 citation edges (100%)" in text and "citation_contexts_missing_text=2" in text
