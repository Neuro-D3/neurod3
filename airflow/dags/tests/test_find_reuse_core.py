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
        self.headers_sent = []

    def get(self, url, timeout=None, headers=None, **kwargs):
        self.urls.append(url)
        self.headers_sent.append(headers)
        return FakeResponse({"ok": True})


class TestOpenalexApiKey:
    def test_key_is_sent_as_bearer_to_openalex_only(self, monkeypatch):
        monkeypatch.setenv("OPENALEX_API_KEY", "abc123")
        assert F.openalex_request_headers("https://api.openalex.org/works/doi:10.1/x") == {"Authorization": "Bearer abc123"}
        assert F.openalex_request_headers("https://api.crossref.org/works/10.1/x") == {}

    def test_no_key_means_no_header(self, monkeypatch):
        monkeypatch.delenv("OPENALEX_API_KEY", raising=False)
        assert F.openalex_request_headers("https://api.openalex.org/works") == {}
        monkeypatch.setenv("OPENALEX_API_KEY", "   ")
        assert F.openalex_request_headers("https://api.openalex.org/works") == {}

    def test_http_get_json_attaches_it(self, monkeypatch):
        monkeypatch.setenv("OPENALEX_API_KEY", "abc123")
        monkeypatch.delenv("PAPER_FETCHER_CONTACT_EMAIL", raising=False)
        monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
        session = FakeSession()
        F.http_get_json(session, "https://api.openalex.org/works/doi:10.1/x", min_interval_seconds=0)
        F.http_get_json(session, "https://api.crossref.org/works/10.1/x", min_interval_seconds=0)
        assert session.headers_sent == [{"Authorization": "Bearer abc123"}, None]


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


class ThrottledResponse:
    def __init__(self, status_code, retry_after=None):
        self.status_code = status_code
        self.headers = {"Retry-After": str(retry_after)} if retry_after is not None else {}
        self.text = "rate limited"

    def json(self):
        return {}

    def raise_for_status(self):
        raise AssertionError("should not be called for a 429")


class SequenceSession:
    """Returns the given responses in order."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url, timeout=None, **kwargs):
        self.calls += 1
        return self._responses.pop(0)


class TestQuotaExhaustion:
    def test_long_retry_after_raises_instead_of_sleeping(self, monkeypatch):
        slept = []
        monkeypatch.setattr(F.time, "sleep", lambda s: slept.append(s))
        tel = F.Telemetry()
        session = SequenceSession([ThrottledResponse(429, retry_after=35004)])
        with pytest.raises(F.ApiQuotaExhausted) as exc:
            F.http_get_json(session, "https://api.openalex.org/works?filter=x", min_interval_seconds=0, telemetry=tel)
        assert slept == []
        assert session.calls == 1
        assert exc.value.retry_after_seconds == 35004
        assert exc.value.status == 429
        assert "api.openalex.org" in str(exc.value)
        assert "UTC" in str(exc.value)
        assert tel.api_quota_exhausted_count == 1
        assert tel.api_429_count == 1

    def test_short_retry_after_is_honoured_then_succeeds(self, monkeypatch):
        slept = []
        monkeypatch.setattr(F.time, "sleep", lambda s: slept.append(s))
        tel = F.Telemetry()
        session = SequenceSession([ThrottledResponse(429, retry_after=12), FakeResponse({"ok": 1})])
        out = F.http_get_json(session, "https://api.crossref.org/works/x", min_interval_seconds=0, telemetry=tel)
        assert out == {"ok": 1}
        assert slept == [12.0]
        assert tel.api_quota_exhausted_count == 0
        assert tel.api_retry_count == 1

    def test_threshold_is_five_minutes(self):
        assert F.MAX_RETRY_AFTER_SECONDS == 300.0


class TestStripNul:
    def test_removes_nul_only(self):
        assert F.strip_nul("a\x00b\x00c") == "abc"
        assert F.strip_nul("tabs\tand\nnewlines\r stay") == "tabs\tand\nnewlines\r stay"

    def test_passthrough_for_clean_or_non_strings(self):
        s = "clean"
        assert F.strip_nul(s) is s
        assert F.strip_nul(None) is None
        assert F.strip_nul("") == ""


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
