"""dandiset_asset_count: empty means both draft and published hold nothing (find_reuse holds_data)."""

import pytest

pytest.importorskip("airflow")

import dandi_ingestion as DI  # noqa: E402


def _ds(published=None, draft=None):
    return {"identifier": "000999", "most_recent_published_version": published, "draft_version": draft}


def test_empty_when_both_versions_are_empty():
    assert DI.dandiset_asset_count(_ds({"asset_count": 0, "size": 0}, {"asset_count": 0, "size": 0})) == 0


def test_files_only_in_the_draft_are_not_empty():
    # The published version shown on the site is empty, but the draft has data.
    assert DI.dandiset_asset_count(_ds({"asset_count": 0, "size": 0}, {"asset_count": 12, "size": 5})) == 12


def test_draft_only_dandiset():
    assert DI.dandiset_asset_count(_ds(None, {"asset_count": 3})) == 3
    assert DI.dandiset_asset_count(_ds(None, {"asset_count": 0})) == 0


def test_size_without_assets_is_unknown_not_empty():
    assert DI.dandiset_asset_count(_ds({"asset_count": 0, "size": 1024}, None)) is None


def test_no_counts_reported_is_unknown():
    assert DI.dandiset_asset_count(_ds(None, None)) is None
    assert DI.dandiset_asset_count(_ds({"size": 0}, {})) is None


def test_parse_dandiset_stores_the_fullest_count():
    row = DI.parse_dandiset(_ds({"asset_count": 0, "name": "Real title", "version": "0.230101.0000"},
                                {"asset_count": 7, "name": "Real title", "version": "draft"}))
    assert row["asset_count"] == 7
