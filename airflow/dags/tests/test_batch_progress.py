"""
Tests for utils/batch_progress.py and the progress lines the four paper-mapping
DAGs log: resolve, citation and context batches, and the run summary's OpenAlex
budget. No database, no network.
"""

import importlib
import logging
import time
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

    def test_request_counts_can_come_from_a_dict_and_name_their_source(self):
        # The resolve step keeps its counts in a dict, and they cover Crossref and DataCite too.
        tel = {"total_requests": 40, "api_429_count": 1, "api_retry_count": 2}
        p = BatchProgress("Resolve batch 1", 3, "datasets", clock=FakeClock(), telemetry=tel, requests_label="API")
        assert "API requests=40 429s=1 retries=2" in p.line()

    def test_ticking_keeps_a_heartbeat_through_one_long_call(self, caplog):
        log = logging.getLogger("test.batch_progress")
        p = BatchProgress("Resolve batch 1", 3, "datasets", every_seconds=0.05, log=log)
        with caplog.at_level(logging.INFO, logger="test.batch_progress"):
            with p.ticking("ds003509"):
                time.sleep(0.3)
            beats = sum("ds003509: still working" in r.getMessage() for r in caplog.records)
            time.sleep(0.15)
            after = sum("ds003509: still working" in r.getMessage() for r in caplog.records)
        assert beats >= 2
        assert after == beats  # the heartbeat stops with the block


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

    def fetchone(self):
        return None

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
    def fake_budget(session=None, telemetry=None):
        if telemetry is not None:  # the real probe counts itself too
            telemetry.total_requests += 1
        return {"remaining": 9000, "limit": 10000}

    monkeypatch.setattr(mod, "fetch_openalex_budget", fake_budget)
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
    # The budget probes at the start and end are OpenAlex requests too.
    assert out["telemetry"]["total_requests"] == 2
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


# archive: (resolver, persist function, the dataset metadata row the batch reads)
RESOLVE = {
    "dandi": ("resolve_papers_for_dandiset", "_persist_resolved_records",
              lambda ds, title: (ds, title, "desc", "https://x", None, "draft")),
    "crcns": ("resolve_papers_for_crcns_dataset", "_persist_crcns_records",
              lambda ds, title: (ds, title, "desc", "https://x", None)),
    "openneuro": ("resolve_papers_for_openneuro_dataset", "_persist_openneuro_records",
                  lambda ds, title: (ds, title, "desc", None)),
    "sparc": ("resolve_papers_for_sparc_dataset", "_persist_sparc_records",
              lambda ds, title: (ds, title, "desc", "https://x", None)),
}


class FakeResolution:
    def __init__(self, papers):
        self.papers = papers
        self.reason = None if papers else "no_papers_found"
        self.error = None
        self.telemetry = {"total_requests": 3, "api_429_count": 0, "api_retry_count": 1}


def test_resolve_batch_logs_each_dataset_and_the_finish(dag_module, monkeypatch, caplog, tmp_path):
    archive, mod = dag_module
    resolver, persist, meta = RESOLVE[archive]
    # ds9 has no metadata row: it gets a "skipped" line rather than none.
    monkeypatch.setattr(mod, "get_db_connection", fake_db([meta("ds1", "Dataset one"), meta("ds2", "Dataset two")]))
    monkeypatch.setattr(mod, resolver,
                        lambda **kw: FakeResolution([{"doi": "10.1/p"}] if "ds1" in kw.values() else []))
    monkeypatch.setattr(mod, persist, lambda **kw: {})
    monkeypatch.setattr(mod, "_get_output_root", lambda: tmp_path)

    with caplog.at_level(logging.INFO):
        out = mod.resolve_and_persist_batch(batch_index=4, dataset_ids=["ds1", "ds9", "ds2"], run_id="r", params={})

    assert out["resolved_mappings"] == 1
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "Resolve batch 4: 0/3 datasets (0%) | papers_found=0 unresolved=0 | API requests=0" in text
    assert "starting ds1 'Dataset one'" in text
    assert "Resolve batch 4: 2/3 datasets (66%) | papers_found=1 unresolved=1 | API requests=3 429s=0 retries=1" in text
    assert f"ds9: not in {archive}_dataset, skipped" in text
    assert "starting ds2 'Dataset two'" in text
    assert "Resolve batch 4: 3/3 datasets (100%) | papers_found=1 unresolved=2 | API requests=6" in text
    assert "batch finished" in text


class NoXcoms:
    def xcom_pull(self, *a, **k):
        return None


def test_run_summary_ends_with_the_openalex_budget(dag_module, monkeypatch, caplog):
    archive, mod = dag_module
    monkeypatch.setattr(mod, "get_db_connection", fake_db([]))
    monkeypatch.setattr(mod, "backfill_papers_text_status", lambda cursor: None, raising=False)

    with caplog.at_level(logging.INFO):
        mod.summarize_run(ti=NoXcoms(), run_id="manual__1")

    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "- after this run: OpenAlex budget: 9,000 of 10,000 requests remaining today" in text
