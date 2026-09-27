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
        found = P.fetch_author_ids(session, dois, telemetry=F.Telemetry(), min_interval_seconds=0)
        assert len(session.urls) == 2
        assert found == {"10.1/p0": (["A1"], []), "10.1/p59": (["A9"], ["0000-0002-0000-0009"])}

    def test_dois_that_would_break_the_filter_are_skipped(self):
        session = FakeSession([work("10.1/ok", ("A1", None))])
        found = P.fetch_author_ids(session, ["10.1/a|b", "10.1/a,b", "10.1/ok"], telemetry=F.Telemetry(),
                                   min_interval_seconds=0)
        assert found == {"10.1/ok": (["A1"], [])}
        assert parse_qs(urlsplit(session.urls[0]).query)["filter"] == ["doi:10.1/ok"]

    def test_url_asks_only_for_what_is_needed(self):
        url = P.works_url(["10.1038/s41586-023-05828-9", "10.1007/978-3-031-19694-2_10"])
        assert url.startswith("https://api.openalex.org/works?filter=doi:10.1038/s41586-023-05828-9|10.1007/")
        assert "select=doi,authorships" in url and "per-page=50" in url


class TestSelectPapersSql:
    def test_orders_new_primary_and_reused_papers_first(self):
        import paper_author_ids as D

        sql = D.select_papers_sql(["dandi_paper_map"], ["dandi_paper_citation_classifications"])
        assert "author_ids_checked_at IS NULL" in sql
        assert "EXISTS (SELECT 1 FROM dandi_paper_map m WHERE m.paper_doi = p.paper_doi)" in sql
        assert "c.classification = 'REUSE'" in sql
        assert sql.index("dandi_paper_map") < sql.index("dandi_paper_citation_classifications")

    def test_no_tables_yet(self):
        import paper_author_ids as D

        sql = D.select_papers_sql([], [])
        assert "(FALSE) DESC" in sql
