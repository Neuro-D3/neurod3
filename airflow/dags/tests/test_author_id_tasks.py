"""
The author-id step in the paper-mapping DAGs, the backfill DAG, and what
fill_missing_author_ids writes. No network, no database: the DB connection and
the OpenAlex lookup are stubbed.
"""

import importlib
import logging
from contextlib import contextmanager

import pytest

pytest.importorskip("airflow")

import utils.database as D  # noqa: E402
from utils import paper_author_ids as P  # noqa: E402
from utils.find_reuse_core import ApiQuotaExhausted  # noqa: E402

MAPPING_DAGS = {
    "dandi": "dandi_paper_mapping",
    "openneuro": "openneuro_paper_mapping",
    "crcns": "crcns_paper_mapping",
    "sparc": "sparc_paper_mapping",
}


@pytest.mark.parametrize("archive, module", MAPPING_DAGS.items())
class TestMappingDagStep:
    def test_runs_once_citing_papers_are_stored_and_before_the_summary(self, archive, module):
        dag = importlib.import_module(module).dag
        task = dag.get_task("fill_author_ids")
        assert task.upstream_task_ids == {"fetch_and_persist_citations_batch"}
        assert task.downstream_task_ids == {"summarize_run"}
        assert task.pool == "paper_mapping_api_pool"
        assert dag.params["fill_author_ids"] is True

    def test_fills_only_its_own_archive(self, archive, module, monkeypatch):
        mod = importlib.import_module(module)
        calls = []
        monkeypatch.setattr(mod, "fill_missing_author_ids", lambda **kw: calls.append(kw) or {"papers": 2})
        assert mod.fill_author_ids(params={"fill_author_ids": True}) == {"papers": 2}
        assert calls[0]["archives"] == [archive]

    def test_can_be_turned_off(self, archive, module, monkeypatch):
        mod = importlib.import_module(module)
        monkeypatch.setattr(mod, "fill_missing_author_ids", lambda **kw: pytest.fail("should not look anything up"))
        assert mod.fill_author_ids(params={"fill_author_ids": False}) == {"skipped": True}


class TestBackfillDag:
    def test_manual_only_and_covers_every_archive(self, monkeypatch):
        from airflow.timetables.simple import NullTimetable

        mod = importlib.import_module("paper_author_ids_backfill")
        assert isinstance(mod.dag.timetable, NullTimetable)
        calls = []
        monkeypatch.setattr(mod, "check_openalex_budget", lambda params: {})
        monkeypatch.setattr(mod, "fill_missing_author_ids", lambda **kw: calls.append(kw) or {})
        mod.backfill_author_ids(params={"max_papers": 123})
        assert calls == [{"max_papers": 123, "recheck_after_days": 30, "min_interval_seconds": 0.2}]


class FakeDb:
    def __init__(self, tables, to_look_up):
        self.tables = set(tables)
        self.to_look_up = list(to_look_up)
        self.statements = []
        self.budget_probes = 0

    def budget(self, session=None, telemetry=None):
        self.budget_probes += 1
        return {"remaining": 9000, "limit": 10000, "authenticated": True}

    @contextmanager
    def connection(self):
        yield FakeConnection(self)


class FakeConnection:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return FakeCursor(self.db)

    def commit(self):
        return None


class FakeCursor:
    rowcount = 1

    def __init__(self, db):
        self.db = db
        self._rows = []

    def execute(self, sql, params=None):
        text = " ".join(sql.split())
        self.db.statements.append((text, params))
        if "information_schema.tables" in text:
            self._rows = [(t,) for t in params[0] if t in self.db.tables]
        elif text.startswith("SELECT p.paper_doi"):
            self._rows = [(d,) for d in self.db.to_look_up]
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class TestFillMissingAuthorIds:
    @pytest.fixture
    def db(self, monkeypatch):
        def make(tables=("dandi_paper_map", "dandi_paper_citations"), to_look_up=("10.1/a", "10.1/b", "10.1/c")):
            fake = FakeDb(tables, to_look_up)
            monkeypatch.setattr(D, "get_db_connection", fake.connection)
            monkeypatch.setattr(D, "apply_schema_ddl", lambda cursor, ddl: fake.statements.append(("DDL", ddl)))
            monkeypatch.setattr(P, "fetch_openalex_budget", fake.budget)
            return fake
        return make

    def updates(self, fake):
        return [params for text, params in fake.statements if text.startswith("UPDATE papers")]

    def test_writes_ids_marks_unknown_papers_and_leaves_unanswered_ones(self, db, monkeypatch):
        fake = db()
        monkeypatch.setattr(P, "fetch_author_ids", lambda session, dois, **kw: (
            {"10.1/a": (["A1", "A2"], ["0000-0001-0000-0001"])}, {"10.1/a", "10.1/b"}))
        stats = P.fill_missing_author_ids(archives=["dandi"], session=object())
        assert self.updates(fake) == [
            ('["A1", "A2"]', '["0000-0001-0000-0001"]', "10.1/a"),
            (None, None, "10.1/b"),
        ]
        assert {k: stats[k] for k in ("papers", "with_author_ids", "not_in_openalex", "unanswered")} == {
            "papers": 3, "with_author_ids": 1, "not_in_openalex": 1, "unanswered": 1}
        assert any(text == "DDL" for text, _ in fake.statements)

    def test_a_mapping_run_selects_only_its_archive(self, db, monkeypatch):
        fake = db()
        monkeypatch.setattr(P, "fetch_author_ids", lambda session, dois, **kw: ({}, set(dois)))
        P.fill_missing_author_ids(archives=["dandi"], session=object())
        select = next(text for text, _ in fake.statements if text.startswith("SELECT p.paper_doi"))
        assert "FROM dandi_paper_citations s WHERE s.citing_paper_doi = p.paper_doi" in select.split("ORDER BY")[0]

    def test_stops_without_failing_when_the_budget_is_spent(self, db, monkeypatch):
        fake = db()

        def spent(session, dois, **kw):
            raise ApiQuotaExhausted("https://api.openalex.org/works", 3600 * 5, 429)

        monkeypatch.setattr(P, "fetch_author_ids", spent)
        stats = P.fill_missing_author_ids(archives=["dandi"], session=object())
        assert stats["stopped_early"] is True
        assert self.updates(fake) == []

    def test_an_archive_without_paper_tables_does_nothing(self, db, monkeypatch):
        fake = db(tables=())
        monkeypatch.setattr(P, "fetch_author_ids", lambda *a, **kw: pytest.fail("nothing to look up"))
        assert P.fill_missing_author_ids(archives=["sparc"], session=object())["papers"] == 0
        assert not any(text.startswith("SELECT p.paper_doi") for text, _ in fake.statements)
        assert fake.budget_probes == 0  # no request spent just to report the budget

    def test_logs_its_start_and_the_budget_left_at_the_end(self, db, monkeypatch, caplog):
        fake = db()
        monkeypatch.setattr(P, "fetch_author_ids", lambda session, dois, **kw: ({}, set(dois)))
        with caplog.at_level(logging.INFO):
            stats = P.fill_missing_author_ids(archives=["dandi"], session=object())
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "author ids: 0/3 papers (0%)" in text and "starting" in text
        assert "author ids: 3/3 papers (100%)" in text
        assert "done. OpenAlex budget: 9,000 of 10,000 requests remaining today; API key" in text
        assert stats["openalex_remaining"] == 9000 and fake.budget_probes == 1
