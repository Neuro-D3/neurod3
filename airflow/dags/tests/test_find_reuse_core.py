"""
Tests for the OpenAlex polite-pool handling in utils/find_reuse_core.py.

No network: the session is a stub that records the URL it was asked for.
"""

import pytest

from utils import find_reuse_core as F


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
    def __init__(self):
        self.urls = []

    def get(self, url, timeout=None, **kwargs):
        self.urls.append(url)
        return FakeResponse({"ok": True})


@pytest.fixture
def no_email(monkeypatch):
    monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
    monkeypatch.delenv("PAPER_FETCHER_CONTACT_EMAIL", raising=False)


class TestContactEmail:
    def test_fetcher_email_is_used_by_default(self, monkeypatch, no_email):
        monkeypatch.setenv("PAPER_FETCHER_CONTACT_EMAIL", "team@example.org")
        assert F.contact_email() == "team@example.org"

    def test_openalex_specific_override_wins(self, monkeypatch, no_email):
        monkeypatch.setenv("PAPER_FETCHER_CONTACT_EMAIL", "team@example.org")
        monkeypatch.setenv("OPENALEX_MAILTO", "openalex@example.org")
        assert F.contact_email() == "openalex@example.org"

    def test_blank_or_malformed_values_are_ignored(self, monkeypatch, no_email):
        monkeypatch.setenv("PAPER_FETCHER_CONTACT_EMAIL", "   ")
        assert F.contact_email() is None
        monkeypatch.setenv("PAPER_FETCHER_CONTACT_EMAIL", "not-an-address")
        assert F.contact_email() is None


class TestOpenalexPoliteUrl:
    def test_adds_mailto_to_openalex_urls(self):
        out = F.openalex_polite_url("https://api.openalex.org/works/doi:10.1/abc", "me@example.org")
        assert out == "https://api.openalex.org/works/doi:10.1/abc?mailto=me%40example.org"

    def test_keeps_existing_query_parameters(self):
        out = F.openalex_polite_url("https://api.openalex.org/works?filter=cites:W1&per_page=200", "me@example.org")
        assert "filter=cites%3AW1" in out
        assert "per_page=200" in out
        assert "mailto=me%40example.org" in out

    def test_does_not_duplicate_an_existing_mailto(self):
        url = "https://api.openalex.org/works?mailto=other%40example.org"
        assert F.openalex_polite_url(url, "me@example.org") == url

    def test_leaves_other_hosts_alone(self):
        for url in (
            "https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=x",
            "https://api.crossref.org/works/10.1/abc",
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc",
        ):
            assert F.openalex_polite_url(url, "me@example.org") == url

    def test_no_email_means_no_change(self, no_email):
        url = "https://api.openalex.org/works/doi:10.1/abc"
        assert F.openalex_polite_url(url) == url


class TestHttpGetJsonUsesIt:
    def test_openalex_request_carries_the_mailto(self, monkeypatch, no_email):
        monkeypatch.setenv("PAPER_FETCHER_CONTACT_EMAIL", "me@example.org")
        session = FakeSession()
        out = F.http_get_json(session, "https://api.openalex.org/works/doi:10.1/abc", min_interval_seconds=0)
        assert out == {"ok": True}
        assert session.urls == ["https://api.openalex.org/works/doi:10.1/abc?mailto=me%40example.org"]

    def test_other_requests_are_untouched(self, monkeypatch, no_email):
        monkeypatch.setenv("PAPER_FETCHER_CONTACT_EMAIL", "me@example.org")
        session = FakeSession()
        F.http_get_json(session, "https://api.crossref.org/works/10.1/abc", min_interval_seconds=0)
        assert session.urls == ["https://api.crossref.org/works/10.1/abc"]
