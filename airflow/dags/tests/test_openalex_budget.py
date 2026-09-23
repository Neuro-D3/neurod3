"""Tests for utils/openalex_budget.py. No network: the probe session is stubbed."""

import pytest

from utils import openalex_budget as B


HEADERS = {
    "X-RateLimit-Cost-USD": "0.0001",
    "X-RateLimit-Credits-Used": "571",
    "X-RateLimit-Limit": "10000",
    "X-RateLimit-Limit-USD": "1",
    "X-RateLimit-Remaining": "9429",
    "X-RateLimit-Remaining-USD": "0.9429",
    "X-RateLimit-Reset": "32375",
}


class FakeResponse:
    def __init__(self, status_code=200, headers=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text


class FakeSession:
    def __init__(self, response=None, exc=None):
        self._response = response
        self._exc = exc
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append((url, headers))
        if self._exc:
            raise self._exc
        return self._response


@pytest.fixture
def keyed(monkeypatch):
    monkeypatch.setenv("OPENALEX_API_KEY", "k")
    monkeypatch.setenv("PAPER_FETCHER_CONTACT_EMAIL", "me@example.org")
    monkeypatch.delenv("OPENALEX_MAILTO", raising=False)


class TestParseBudgetHeaders:
    def test_reads_every_field(self):
        b = B.parse_budget_headers(HEADERS)
        assert b["limit"] == 10000 and b["remaining"] == 9429 and b["used"] == 571
        assert b["cost_usd"] == 0.0001 and b["limit_usd"] == 1.0 and b["remaining_usd"] == 0.9429
        assert b["reset_seconds"] == 32375.0
        assert b["resets_at"] is not None

    def test_header_names_are_case_insensitive(self):
        b = B.parse_budget_headers({"x-ratelimit-remaining": "5", "X-RATELIMIT-LIMIT": "10"})
        assert (b["remaining"], b["limit"]) == (5, 10)

    def test_missing_or_bad_values_are_none(self):
        b = B.parse_budget_headers({"X-RateLimit-Remaining": "lots"})
        assert b["remaining"] is None and b["limit"] is None and b["resets_at"] is None


class TestFetchOpenalexBudget:
    def test_probe_is_authenticated_and_polite(self, keyed):
        s = FakeSession(FakeResponse(200, HEADERS))
        b = B.fetch_openalex_budget(session=s)
        url, headers = s.calls[0]
        assert url.startswith("https://api.openalex.org/works?per-page=1")
        assert "mailto=me%40example.org" in url
        assert headers == {"Authorization": "Bearer k"}
        assert b["authenticated"] is True and b["status"] == 200 and b["remaining"] == 9429

    def test_network_failure_is_reported_not_raised(self, keyed):
        import requests
        b = B.fetch_openalex_budget(session=FakeSession(exc=requests.ConnectionError("down")))
        assert b["status"] is None and b["remaining"] is None and "down" in b["error"]

    def test_429_still_yields_headers(self, keyed):
        b = B.fetch_openalex_budget(session=FakeSession(FakeResponse(429, {"X-RateLimit-Remaining": "0", "X-RateLimit-Limit": "10000"}, "spent")))
        assert b["status"] == 429 and b["remaining"] == 0 and b["error"] == "spent"


class TestCheckOpenalexBudget:
    def test_logs_and_passes_when_plenty_remains(self, keyed, caplog):
        with caplog.at_level("INFO"):
            b = B.check_openalex_budget({"min_openalex_requests": 200}, session=FakeSession(FakeResponse(200, HEADERS)))
        assert b["remaining"] == 9429
        assert any("9,429 of 10,000 requests remaining" in r.message for r in caplog.records)

    def test_aborts_below_the_floor(self, keyed):
        pytest.importorskip("airflow")
        from airflow.exceptions import AirflowFailException
        low = dict(HEADERS, **{"X-RateLimit-Remaining": "150"})
        with pytest.raises(AirflowFailException) as exc:
            B.check_openalex_budget({"min_openalex_requests": 200}, session=FakeSession(FakeResponse(200, low)))
        assert "150 of 10,000" in str(exc.value) and "min_openalex_requests=200" in str(exc.value)

    def test_429_probe_aborts(self, keyed):
        pytest.importorskip("airflow")
        from airflow.exceptions import AirflowFailException
        with pytest.raises(AirflowFailException):
            B.check_openalex_budget({}, session=FakeSession(FakeResponse(429, {"X-RateLimit-Remaining": "0", "X-RateLimit-Limit": "10000"})))

    def test_floor_zero_only_logs(self, keyed):
        low = dict(HEADERS, **{"X-RateLimit-Remaining": "3"})
        b = B.check_openalex_budget({"min_openalex_requests": 0}, session=FakeSession(FakeResponse(200, low)))
        assert b["remaining"] == 3

    def test_unreadable_probe_does_not_block(self, keyed):
        import requests
        b = B.check_openalex_budget({"min_openalex_requests": 200}, session=FakeSession(exc=requests.Timeout("slow")))
        assert b["remaining"] is None

    def test_keyless_is_warned(self, monkeypatch, caplog):
        monkeypatch.delenv("OPENALEX_API_KEY", raising=False)
        monkeypatch.delenv("PAPER_FETCHER_CONTACT_EMAIL", raising=False)
        monkeypatch.delenv("OPENALEX_MAILTO", raising=False)
        with caplog.at_level("WARNING"):
            B.check_openalex_budget({"min_openalex_requests": 0}, session=FakeSession(FakeResponse(200, HEADERS)))
        assert any("NO API KEY" in r.message and "OPENALEX_API_KEY" in r.message for r in caplog.records)
