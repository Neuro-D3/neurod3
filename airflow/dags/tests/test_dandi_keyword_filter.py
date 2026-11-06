"""keyword_filter_dataset: the runtime exclude_keywords filter in dandi_paper_mapping.

It used to build its pattern as rf"\b...\b", which looks for a literal
backslash, so it never matched anything.
"""

import pytest

pytest.importorskip("airflow")

import dandi_paper_mapping as DM  # noqa: E402


def test_keyword_in_title_is_filtered():
    assert DM.keyword_filter_dataset("Dummy upload", None, ("dummy",)) == (True, "keyword:dummy")


def test_keyword_in_description_is_filtered():
    assert DM.keyword_filter_dataset("Mouse V1", "a test dataset", ("test",)) == (True, "keyword:test")


def test_keyword_must_be_a_whole_word():
    assert DM.keyword_filter_dataset("Contested regions", None, ("test",)) == (False, None)


def test_no_keywords_or_no_text_is_kept():
    assert DM.keyword_filter_dataset("Dummy upload", None, ()) == (False, None)
    assert DM.keyword_filter_dataset(None, None, ("dummy",)) == (False, None)
