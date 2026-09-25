"""Tests for the deterministic citation-context helpers in utils/paper_citations.py."""

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
