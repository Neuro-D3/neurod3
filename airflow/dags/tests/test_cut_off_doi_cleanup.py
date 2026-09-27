"""Tests for utils/cut_off_doi_cleanup.py: which stored primary DOIs count as cut off."""

from utils.cut_off_doi_cleanup import cut_off_dois


class TestCutOffDois:
    def test_unresolved_prefix_of_a_doi_in_the_description(self):
        # OpenNeuro ds003509: mapped from the 256-character description.
        mapped = {"10.1016/j.neur": False, "10.1016/j.cortex.2017.02.021": True}
        text = ["10.1016/j.cortex.2017.02.021", "10.1016/j.neuropsychologia.2018.05.020"]
        assert cut_off_dois(mapped, text) == ["10.1016/j.neur"]

    def test_unresolved_prefix_of_another_mapped_doi(self):
        mapped = {"10.1093/cerc": False, "10.1093/cercor/bhab001": True}
        assert cut_off_dois(mapped, []) == ["10.1093/cerc"]

    def test_resolved_dois_are_never_cut_off(self):
        # OpenNeuro ds008022: the README has the DOI in markdown bold (`...-7**`).
        mapped = {"10.1038/s41593-025-02037-7": True}
        assert cut_off_dois(mapped, ["10.1038/s41593-025-02037-7**"]) == []

    def test_unresolved_dois_with_nothing_longer_are_kept(self):
        assert cut_off_dois({"10.1101/2021.02.12.430858": False}, ["10.1016/j.neuron.2019.09.045"]) == []
