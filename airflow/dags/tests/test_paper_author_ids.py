"""Tests for utils/paper_author_ids.py and the paper_author_ids DAG's selection. No network."""

from urllib.parse import parse_qs, urlsplit

import pytest

from utils import find_reuse_core as F
from utils import paper_author_ids as P


def work(doi, *authors):
    return {
        "doi": f"https://doi.org/{doi}",
        "authorships": [
            {"author": {"id": f"https://openalex.org/{aid}", "orcid": f"https://orcid.org/{orcid}" if orcid else None}}
            for aid, orcid in authors
        ],
    }


class FakeResponse:
    status_code = 200
    headers: dict = {}

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class FakeSession:
    """Answers each works query with the works whose DOI it asked for."""

    def __init__(self, works):
        self.works = {w["doi"].split("doi.org/")[1].lower(): w for w in works}
        self.urls = []

    def get(self, url, timeout=None, headers=None):
        self.urls.append(url)
        dois = parse_qs(urlsplit(url).query)["filter"][0].split("doi:", 1)[1]
        asked = dois.lower().split("|")
        return FakeResponse({"results": [self.works[d] for d in asked if d in self.works]})


class TestAuthorIdsFromWork:
    def test_ids_and_orcids_in_author_order(self):
        w = work("10.1/a", ("A1", "0000-0001-0000-0001"), ("A2", None), ("A1", "0000-0001-0000-0001"))
        assert P.author_ids_from_work(w) == (["A1", "A2"], ["0000-0001-0000-0001"])

    def test_missing_or_odd_authorships(self):
        assert P.author_ids_from_work({}) == ([], [])
        assert P.author_ids_from_work({"authorships": [None, {"author": None}, {"author": {"id": ""}}]}) == ([], [])


class TestFetchAuthorIds:
    @pytest.fixture(autouse=True)
    def fast(self, monkeypatch):
        monkeypatch.setattr(F, "_throttle", lambda **k: None)
        monkeypatch.delenv("OPENALEX_API_KEY", raising=False)

    def test_one_request_per_fifty_dois_and_results_by_lowercase_doi(self):
        dois = [f"10.1/P{i}" for i in range(60)]
        session = FakeSession([work("10.1/p0", ("A1", None)), work("10.1/p59", ("A9", "0000-0002-0000-0009"))])
        found, answered = P.fetch_author_ids(session, dois, telemetry=F.Telemetry(), min_interval_seconds=0)
        assert len(session.urls) == 2
        assert found == {"10.1/p0": (["A1"], []), "10.1/p59": (["A9"], ["0000-0002-0000-0009"])}
        assert answered == set(dois)

    def test_dois_that_would_break_the_filter_are_answered_as_unknown(self):
        session = FakeSession([work("10.1/ok", ("A1", None))])
        found, answered = P.fetch_author_ids(session, ["10.1/a|b", "10.1/a,b", "10.1/ok"], telemetry=F.Telemetry(),
                                             min_interval_seconds=0)
        assert found == {"10.1/ok": (["A1"], [])}
        assert answered == {"10.1/a|b", "10.1/a,b", "10.1/ok"}
        assert parse_qs(urlsplit(session.urls[0]).query)["filter"] == ["doi:10.1/ok"]

    def test_a_failed_request_leaves_its_dois_unanswered(self):
        class DownSession:
            def get(self, url, timeout=None, headers=None):
                raise F.requests.ConnectionError("down")

        found, answered = P.fetch_author_ids(DownSession(), ["10.1/a", "10.1/b"], telemetry=F.Telemetry(),
                                             min_interval_seconds=0, max_retries=1, backoff_seconds=0)
        assert (found, answered) == ({}, set())

    def test_url_asks_only_for_what_is_needed(self):
        url = P.works_url(["10.1038/s41586-023-05828-9", "10.1007/978-3-031-19694-2_10"])
        assert url.startswith("https://api.openalex.org/works?filter=doi:10.1038/s41586-023-05828-9|10.1007/")
        assert "select=doi,authorships" in url and "per-page=50" in url


class TestSelectPapersSql:
    def test_a_mapping_run_looks_only_at_its_archive(self):
        sql = P.select_papers_sql(
            [("dandi_paper_map", "paper_doi"), ("dandi_paper_citations", "citing_paper_doi")],
            ["dandi_paper_map"], ["dandi_paper_citation_classifications"],
        )
        where = sql.split("ORDER BY")[0]
        assert "EXISTS (SELECT 1 FROM dandi_paper_citations s WHERE s.citing_paper_doi = p.paper_doi)" in where
        assert "EXISTS (SELECT 1 FROM dandi_paper_map s WHERE s.paper_doi = p.paper_doi)" in where
        assert "openneuro" not in sql

    def test_the_backfill_looks_at_every_paper_primary_and_reused_first(self):
        sql = P.select_papers_sql([], ["dandi_paper_map", "sparc_paper_map"],
                                  ["dandi_paper_citation_classifications"])
        where, order = sql.split("ORDER BY")
        assert "EXISTS" not in where
        assert "author_ids_checked_at IS NULL" in where
        assert order.index("dandi_paper_map") < order.index("c.classification = 'REUSE'")

    def test_no_tables_yet(self):
        assert "(FALSE) DESC" in P.select_papers_sql([], [], [])
