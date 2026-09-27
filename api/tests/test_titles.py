"""Paper titles are served without publisher markup (the DAGs clean new ones as they store them)."""

import main as M


class TestCleanTitle:
    def test_drops_known_inline_tags_and_keeps_their_text(self):
        assert M._clean_title("<scp>Medication-invariant</scp> resting activity") == "Medication-invariant resting activity"
        assert M._clean_title("Ca<sup>2+</sup> in <i>Mus musculus</i>") == "Ca2+ in Mus musculus"

    def test_decodes_entities_and_keeps_comparisons(self):
        assert M._clean_title("Mice &amp; men") == "Mice & men"
        assert M._clean_title("When x < y and y > z") == "When x < y and y > z"

    def test_non_strings_pass_through(self):
        assert M._clean_title(None) is None
        assert M._clean_title(42) == 42


class TestCleanPaperTitles:
    def test_cleans_paper_title_fields_only(self):
        rows = [{
            "paper_title": "<i>A</i> paper",
            "primary_paper_title": "B &amp; C",
            "citing_paper_title": "<scp>D</scp>",
            "title": "<sub>E</sub>",
            "dataset_title": "<i>left alone</i>",
        }]
        assert M._clean_paper_titles(rows) == [{
            "paper_title": "A paper",
            "primary_paper_title": "B & C",
            "citing_paper_title": "D",
            "title": "E",
            "dataset_title": "<i>left alone</i>",
        }]
