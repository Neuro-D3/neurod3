"""Tests for the deterministic citation-context helpers in utils/paper_citations.py."""

import pytest

from utils import paper_citations as C


class TestExtractContext:
    def test_window_is_sentence_aligned(self):
        text = "First sentence. Second one mentions the dataset here. Third sentence follows."
        pos = text.index("dataset")
        ctx = C.extract_context(text, pos, context_chars=30)
        assert "mentions the dataset here" in ctx["context"]
        assert ctx["context"] in text  # a contiguous slice of the source
        assert ctx["start"] <= pos < ctx["end"]
        assert ctx["citation_position"] == pos

    def test_nul_characters_are_removed_from_the_excerpt(self):
        # PDF extraction can leave U+0000 in the text; jsonb refuses to store it.
        text = "Prior work.\x00 We downloaded \x00the dataset from DANDI. Then more."
        pos = text.index("DANDI")
        ctx = C.extract_context(text, pos, context_chars=40)
        assert "\x00" not in ctx["context"]
        assert "downloaded the dataset" in ctx["context"]


class TestGetCitingPapersSkipsNonPapers:
    """OpenAlex lists eLife assessments, OSF registrations and NEMAR records as citing works."""

    def test_non_paper_dois_are_skipped_and_counted(self, monkeypatch):
        from utils.find_reuse_core import Telemetry

        page = {
            "results": [
                {"id": "https://openalex.org/W1", "doi": "https://doi.org/10.7554/eLife.1.sa0"},
                {"id": "https://openalex.org/W2", "doi": "https://doi.org/10.1038/s41586-023-06031-6"},
                {"id": "https://openalex.org/W3", "doi": "https://doi.org/10.17605/osf.io/abcde"},
            ],
            "meta": {"next_cursor": None},
        }
        monkeypatch.setattr(C, "http_get_json", lambda *a, **k: page)
        tel = Telemetry()
        out = C.get_citing_papers(None, "W99", "2020-01-01", telemetry=tel, max_results=10,
                                  min_interval_seconds=0)
        assert [p["doi"] for p in out] == ["10.1038/s41586-023-06031-6"]
        assert tel.non_paper_dois_skipped == 2


class TestOpenAlexFailuresRaise:
    """A citing-paper list cut short by OpenAlex must raise, not look complete."""

    def test_failure_on_a_later_page_raises_with_the_count(self, monkeypatch):
        from utils.find_reuse_core import HttpFetchFailed, OpenAlexError, Telemetry

        pages = [
            {"results": [{"id": "https://openalex.org/W2", "doi": "https://doi.org/10.1/a"}],
             "meta": {"next_cursor": "abc"}},
            HttpFetchFailed("https://api.openalex.org/works", "HTTPError: 500", 500),
        ]

        def fake_get(*a, **k):
            assert k.get("raise_on_failure") is True
            item = pages.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        monkeypatch.setattr(C, "http_get_json", fake_get)
        with pytest.raises(OpenAlexError, match="after 1 papers"):
            C.get_citing_papers(None, "W99", "2020-01-01", telemetry=Telemetry(), max_results=10,
                                min_interval_seconds=0)

    def test_paper_data_raises_only_when_asked(self, monkeypatch):
        from utils.find_reuse_core import HttpFetchFailed, OpenAlexError, Telemetry

        def fake_get(*a, **k):
            if k.get("raise_on_failure"):
                raise HttpFetchFailed("u", "HTTPError: 500", 500)
            return None

        monkeypatch.setattr(C, "http_get_json", fake_get)
        assert C.get_openalex_paper_data(None, "10.1/a", telemetry=Telemetry())["openalex_id"] is None
        with pytest.raises(OpenAlexError):
            C.get_openalex_paper_data(None, "10.1/a", telemetry=Telemetry(), raise_on_failure=True)
