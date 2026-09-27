"""Tests for utils/titles.py."""

import pytest

from utils.titles import clean_title


class TestCleanTitle:
    def test_drops_markup_publishers_leave_in_titles(self):
        assert clean_title(
            "<scp>Medication\u2010invariant</scp> resting aperiodic and periodic neural activity"
        ) == "Medication\u2010invariant resting aperiodic and periodic neural activity"
        assert clean_title("Gene <i>Foxp2</i> in <i>Mus musculus</i>") == "Gene Foxp2 in Mus musculus"
        assert clean_title("Ca<sup>2+</sup> imaging of H<sub>2</sub>O") == "Ca2+ imaging of H2O"

    def test_drops_tags_with_attributes_and_mathml(self):
        assert clean_title('A <span class="x">bold</span> claim') == "A bold claim"
        assert clean_title(
            'Decoding <mml:math xmlns:mml="http://www.w3.org/1998/Math/MathML"><mml:mi>\u03b8</mml:mi></mml:math> rhythms'
        ) == "Decoding \u03b8 rhythms"

    def test_decodes_entities_including_escaped_markup(self):
        assert clean_title("Mice &amp; men") == "Mice & men"
        assert clean_title("&lt;i&gt;Drosophila&lt;/i&gt; larvae") == "Drosophila larvae"

    def test_keeps_comparisons_that_look_like_brackets(self):
        assert clean_title("When x < y and y > z") == "When x < y and y > z"
        assert clean_title("p &lt; 0.05 is not enough") == "p < 0.05 is not enough"

    def test_collapses_whitespace(self):
        assert clean_title("  Neural\n  population   dynamics ") == "Neural population dynamics"

    @pytest.mark.parametrize("value, expected", [(None, None), ("", None), ("   ", None), ("<i></i>", None)])
    def test_empty_values(self, value, expected):
        assert clean_title(value) == expected
