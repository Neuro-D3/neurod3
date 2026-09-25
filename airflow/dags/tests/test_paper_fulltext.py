"""
Tests for the paper-text-fetcher helpers in utils/paper_fulltext.py.

No network access and no real ``paper_text_fetcher`` needed: the fetcher is
replaced by a stub, and the on-disk caches are built under ``tmp_path``.
"""

import json
from pathlib import Path

import pytest

from utils import paper_fulltext as P


class FakeCache:
    def __init__(self, cache_dir, entries=None):
        self.cache_dir = Path(cache_dir)
        self._entries = entries or {}

    def get(self, doi):
        return self._entries.get(doi)

    def path_for(self, doi):
        return self.cache_dir / f"{doi.replace('/', '%2F')}.json"


class FakeFetcher:
    def __init__(self, cache_dir, result=None, exc=None, entries=None):
        self.cache = FakeCache(cache_dir, entries)
        self._result = result
        self._exc = exc
        self.calls = []

    def get_paper_text_detailed(self, doi):
        self.calls.append(doi)
        if self._exc is not None:
            raise self._exc
        return self._result


class FakeCursor:
    def __init__(self, cache_key):
        self._cache_key = cache_key
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchone(self):
        return (self._cache_key,)


@pytest.fixture(autouse=True)
def _reset_thread_local(monkeypatch):
    """Each test gets a fresh per-thread fetcher slot."""
    monkeypatch.setattr(P, "_fetcher_local", type(P._fetcher_local)())
    monkeypatch.setattr(P, "_fetcher_import_warned", False)


def install_fake_fetcher(monkeypatch, fetcher):
    monkeypatch.setattr(P, "get_paper_fetcher", lambda cache_dir=None: fetcher)
    return fetcher


# --------------------------------------------------------------------------- #
# fetch_fulltext_detailed
# --------------------------------------------------------------------------- #

class TestFetchFulltextDetailed:
    KEYS = {"text", "source", "status", "has_full_text", "reason", "from_cache"}

    def test_full_text_passes_through(self, monkeypatch, tmp_path):
        install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={
            "text": "body " * 2000, "source": "europe_pmc", "status": "full_text",
            "has_full_text": True, "reason": None, "from_cache": False,
        }))
        out = P.fetch_fulltext_detailed("10.1000/abc")
        assert set(out) == self.KEYS
        assert out["status"] == P.TEXT_STATUS_FULL
        assert out["has_full_text"] is True
        assert out["text"].startswith("body")
        assert out["source"] == "europe_pmc"

    def test_metadata_only_never_returns_text(self, monkeypatch, tmp_path):
        install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={
            "text": "just an abstract", "source": "crossref", "status": "metadata_only",
            "has_full_text": False, "reason": "closed access", "from_cache": True,
        }))
        out = P.fetch_fulltext_detailed("10.1000/abc")
        assert out["status"] == P.TEXT_STATUS_METADATA
        assert out["text"] is None
        assert out["has_full_text"] is False
        assert out["from_cache"] is True
        assert out["reason"] == "closed access"

    def test_invalid_doi_is_unavailable_without_a_call(self, monkeypatch, tmp_path):
        fetcher = install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={}))
        out = P.fetch_fulltext_detailed("not a doi")
        assert out["status"] == P.TEXT_STATUS_UNAVAILABLE
        assert out["reason"] == "invalid_doi"
        assert fetcher.calls == []

    def test_missing_package_is_unavailable_not_an_exception(self, monkeypatch):
        monkeypatch.setattr(P, "get_paper_fetcher", lambda cache_dir=None: None)
        out = P.fetch_fulltext_detailed("10.1000/abc")
        assert out["status"] == P.TEXT_STATUS_UNAVAILABLE
        assert out["reason"] == "paper_text_fetcher_not_installed"
        assert set(out) == self.KEYS

    def test_fetcher_exception_is_unavailable_and_counted(self, monkeypatch, tmp_path):
        install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, exc=RuntimeError("boom")))
        telemetry = P.Telemetry()
        out = P.fetch_fulltext_detailed("10.1000/abc", telemetry=telemetry)
        assert out["status"] == P.TEXT_STATUS_UNAVAILABLE
        assert out["reason"].startswith("fetch_error")
        assert telemetry.total_requests == 1
        assert telemetry.api_retry_count == 1

    def test_doi_is_normalized_before_the_call(self, monkeypatch, tmp_path):
        fetcher = install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={
            "text": None, "source": None, "status": "unavailable",
            "has_full_text": False, "reason": "nothing", "from_cache": False,
        }))
        P.fetch_fulltext_detailed("https://doi.org/10.1000/ABC")
        assert fetcher.calls == ["10.1000/ABC"]  # prefix stripped; case preserved


# --------------------------------------------------------------------------- #
# load_cached_paper_text
# --------------------------------------------------------------------------- #

def write_mapping_cache(root: Path, key: str, payload: dict) -> Path:
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class TestLoadCachedPaperText:
    KEY = "papers/abc123/latest.json"

    def test_reads_the_mapping_dag_cache_under_the_first_root_that_has_it(
            self, monkeypatch, tmp_path):
        roots = [tmp_path / "dandi", tmp_path / "openneuro"]
        monkeypatch.setattr(P, "mapping_output_roots", lambda: roots)
        monkeypatch.setattr(P, "get_paper_fetcher", lambda cache_dir=None: None)
        write_mapping_cache(roots[1], self.KEY, {"full_text": "the paper body"})

        text = P.load_cached_paper_text(FakeCursor(self.KEY), "10.1000/abc")
        assert text == "the paper body"

    def test_legacy_text_field_is_accepted(self, monkeypatch, tmp_path):
        roots = [tmp_path / "dandi"]
        monkeypatch.setattr(P, "mapping_output_roots", lambda: roots)
        monkeypatch.setattr(P, "get_paper_fetcher", lambda cache_dir=None: None)
        write_mapping_cache(roots[0], self.KEY, {"text": "legacy body"})

        assert P.load_cached_paper_text(FakeCursor(self.KEY), "10.1000/abc") == "legacy body"

    def test_empty_or_missing_body_is_a_miss(self, monkeypatch, tmp_path):
        roots = [tmp_path / "dandi"]
        monkeypatch.setattr(P, "mapping_output_roots", lambda: roots)
        monkeypatch.setattr(P, "get_paper_fetcher", lambda cache_dir=None: None)
        write_mapping_cache(roots[0], self.KEY, {"full_text": "   ", "full_text_available": False})

        assert P.load_cached_paper_text(FakeCursor(self.KEY), "10.1000/abc") is None

    def test_falls_back_to_the_fetcher_cache_only_for_full_text(
            self, monkeypatch, tmp_path):
        monkeypatch.setattr(P, "mapping_output_roots", lambda: [tmp_path / "dandi"])
        fetcher = FakeFetcher(tmp_path / "ptf", entries={
            "10.1000/full": ("full body", "unpaywall", True),
            "10.1000/abstract": ("abstract only", "crossref", False),
        })
        install_fake_fetcher(monkeypatch, fetcher)

        assert P.load_cached_paper_text(FakeCursor(None), "10.1000/full") == "full body"
        assert P.load_cached_paper_text(FakeCursor(None), "10.1000/abstract") is None
        assert fetcher.calls == []  # cache lookups never fetch

    def test_invalid_doi_short_circuits(self, monkeypatch):
        cursor = FakeCursor(self.KEY)
        assert P.load_cached_paper_text(cursor, "") is None
        assert cursor.queries == []

    def test_papers_lookup_failure_is_not_fatal(self, monkeypatch, tmp_path):
        class BrokenCursor(FakeCursor):
            def execute(self, sql, params=None):
                raise RuntimeError("db down")

        monkeypatch.setattr(P, "mapping_output_roots", lambda: [tmp_path])
        monkeypatch.setattr(P, "get_paper_fetcher", lambda cache_dir=None: None)
        assert P.load_cached_paper_text(BrokenCursor(self.KEY), "10.1000/abc") is None


# --------------------------------------------------------------------------- #
# configuration helpers
# --------------------------------------------------------------------------- #

class TestConfiguration:
    def test_cache_dir_env_override(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PAPER_FETCHER_CACHE_DIR", str(tmp_path / "custom"))
        assert P.fetcher_cache_dir() == tmp_path / "custom"

    def test_cache_dir_default_lives_outside_the_dag_folder(self, monkeypatch):
        # The DAG processor walks everything under dags/; a paper cache there
        # stalls it, so the default is <airflow home>/output, next to dags/.
        monkeypatch.delenv("PAPER_FETCHER_CACHE_DIR", raising=False)
        path = P.fetcher_cache_dir()
        assert path.parts[-2:] == ("output", "paper_text_fetcher")
        assert path.parent.parent == P._airflow_home() == P._dags_dir().parent
        assert P._dags_dir() not in path.parents

    def test_mapping_roots_follow_each_dags_env_var(self, monkeypatch, tmp_path):
        for env_name, _default in P.MAPPING_OUTPUT_ROOTS:
            monkeypatch.delenv(env_name, raising=False)
        monkeypatch.setenv("CRCNS_PAPER_MAPPING_OUTPUT_DIR", str(tmp_path / "crcns_out"))

        roots = P.mapping_output_roots()
        assert len(roots) == 4
        assert roots[2] == tmp_path / "crcns_out"
        assert roots[0] == P._airflow_home() / "output" / "dandi_paper_mapping"

    def test_elsevier_key_is_passed_only_when_set(self, monkeypatch):
        monkeypatch.delenv("ELSEVIER_API_KEY", raising=False)
        monkeypatch.delenv("SCOPUS_API_KEY", raising=False)
        assert P._fetcher_api_keys() == {}
        monkeypatch.setenv("ELSEVIER_API_KEY", "k")
        assert P._fetcher_api_keys() == {"elsevier": "k"}

    def test_missing_package_returns_none_and_warns_once(self, monkeypatch, caplog):
        import builtins
        real_import = builtins.__import__

        def no_ptf(name, *args, **kwargs):
            if name == "paper_text_fetcher":
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", no_ptf)
        with caplog.at_level("WARNING"):
            assert P.get_paper_fetcher() is None
            assert P.get_paper_fetcher() is None
        assert sum("paper_text_fetcher is not installed" in r.message for r in caplog.records) == 1


# --------------------------------------------------------------------------- #
# fetch_fulltext_oa: the mapping DAGs' 4-tuple entry point now delegates
# --------------------------------------------------------------------------- #

class TestFetchFulltextOaDelegation:
    def test_full_text_comes_back_as_available_ok(self, monkeypatch, tmp_path):
        install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={
            "text": "body " * 2000, "source": "europe_pmc+crossref", "status": "full_text",
            "has_full_text": True, "reason": None, "from_cache": True,
        }))
        text, source, available, reason = P.fetch_fulltext_oa(None, "10.1000/abc", telemetry=P.Telemetry())
        assert text.startswith("body")
        assert source == "europe_pmc+crossref"
        assert available is True
        assert reason == "ok"

    def test_metadata_only_keeps_the_status_in_reason(self, monkeypatch, tmp_path):
        install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={
            "text": "abstract", "source": "crossref", "status": "metadata_only",
            "has_full_text": False, "reason": "closed access", "from_cache": True,
        }))
        text, source, available, reason = P.fetch_fulltext_oa(None, "10.1000/abc", telemetry=P.Telemetry())
        assert text is None
        assert source == "crossref"
        assert available is False
        assert reason == "metadata_only: closed access"

    def test_unavailable_keeps_the_status_in_reason(self, monkeypatch, tmp_path):
        install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={
            "text": None, "source": "", "status": "unavailable",
            "has_full_text": False, "reason": "nothing", "from_cache": True,
        }))
        text, source, available, reason = P.fetch_fulltext_oa(None, "10.1000/abc", telemetry=P.Telemetry())
        assert (text, source, available) == (None, "none", False)
        assert reason == "unavailable: nothing"

    def test_without_the_package_the_legacy_path_runs(self, monkeypatch):
        monkeypatch.setattr(P, "get_paper_fetcher", lambda cache_dir=None: None)
        called = {}

        def fake_pmcid(session, doi):
            called["doi"] = doi
            return None  # no PMCID -> legacy path falls through to NCBI, stubbed below

        monkeypatch.setattr(P, "_europe_pmc_find_pmcid", fake_pmcid)
        monkeypatch.setattr(P, "_get_text_with_retries", lambda *a, **k: None)
        text, source, available, reason = P.fetch_fulltext_oa(None, "10.1000/abc", telemetry=P.Telemetry())
        assert called["doi"] == "10.1000/abc"
        assert (text, source, available, reason) == (None, "none", False, "no_oa_fulltext_found")

    def test_invalid_doi_short_circuits_before_any_fetch(self, monkeypatch, tmp_path):
        fetcher = install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={}))
        assert P.fetch_fulltext_oa(None, "nope", telemetry=P.Telemetry()) == (None, "none", False, "invalid_doi")
        assert fetcher.calls == []


class TestNulCharactersNeverLeaveTheHelper:
    """PDF/HTML extraction can leave U+0000 in text; Postgres jsonb rejects it."""

    NUL_TEXT = "intro \x00 methods \x00\x00 data availability " * 300

    def test_fetch_fulltext_detailed_strips_nul(self, monkeypatch, tmp_path):
        install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={
            "text": self.NUL_TEXT, "source": "unpaywall", "status": "full_text",
            "has_full_text": True, "reason": None, "from_cache": False,
        }))
        out = P.fetch_fulltext_detailed("10.1000/abc")
        assert "\x00" not in out["text"] and "methods" in out["text"]

    def test_fetch_fulltext_oa_strips_nul(self, monkeypatch, tmp_path):
        install_fake_fetcher(monkeypatch, FakeFetcher(tmp_path, result={
            "text": self.NUL_TEXT, "source": "unpaywall", "status": "full_text",
            "has_full_text": True, "reason": None, "from_cache": True,
        }))
        text, _s, available, reason = P.fetch_fulltext_oa(None, "10.1000/abc", telemetry=P.Telemetry())
        assert available and reason == "ok" and "\x00" not in text

    def test_cached_text_is_cleaned_on_read(self, monkeypatch, tmp_path):
        roots = [tmp_path / "dandi"]
        monkeypatch.setattr(P, "mapping_output_roots", lambda: roots)
        fetcher = FakeFetcher(tmp_path / "ptf", entries={"10.1000/ptf": ("from \x00 fetcher", "pmc", True)})
        install_fake_fetcher(monkeypatch, fetcher)
        write_mapping_cache(roots[0], "papers/x/latest.json", {"full_text": "from \x00 mapping cache"})
        assert P.load_cached_paper_text(FakeCursor("papers/x/latest.json"), "10.1000/abc") == "from  mapping cache"
        assert P.load_cached_paper_text(FakeCursor(None), "10.1000/ptf") == "from  fetcher"


# --------------------------------------------------------------------------- #
# The real fetcher subclass (only where paper-text-fetcher is installed)
# --------------------------------------------------------------------------- #

class TestD3PaperFetcher:
    def test_new_biorxiv_prefix_is_treated_as_a_preprint(self, monkeypatch, tmp_path):
        pytest.importorskip("paper_text_fetcher")
        monkeypatch.setenv("PAPER_FETCHER_CACHE_DIR", str(tmp_path / "ptf"))
        monkeypatch.setenv("PAPER_FETCHER_CONTACT_EMAIL", "tests@example.org")
        fetcher = P.get_paper_fetcher()
        assert fetcher is not None
        assert fetcher.is_preprint_doi("10.1101/2024.01.01.573000") is True
        assert fetcher.is_preprint_doi("10.64898/2026.07.06.732916") is True
        assert fetcher.is_preprint_doi("10.1016/j.cub.2026.07.028") is False
        assert fetcher.contact_email == "tests@example.org"
        assert str(fetcher.cache.cache_dir) == str(tmp_path / "ptf")

    def test_same_thread_reuses_one_instance(self, monkeypatch, tmp_path):
        pytest.importorskip("paper_text_fetcher")
        monkeypatch.setenv("PAPER_FETCHER_CACHE_DIR", str(tmp_path / "ptf"))
        assert P.get_paper_fetcher() is P.get_paper_fetcher()
