"""
Tests for utils.dataset_status: the junk rules, the status decision, and the
whole-archive refresh. No database: a fake cursor hands back rows and records
the UPDATEs.
"""

import pytest

from utils.dataset_status import (
    FIND_REUSE_TEST_DANDISETS,
    STATUS_EXCLUDED,
    STATUS_MAPPED,
    STATUS_NO_PAPER,
    STATUS_PENDING,
    compute_status,
    dataset_status_ddl,
    junk_reason,
    refresh_dataset_status,
)


class TestJunkReason:
    @pytest.mark.parametrize("title,reason", [
        ("Test", "placeholder_title"),
        ("test", "placeholder_title"),
        ("Testytest", "placeholder_title"),
        ("Test 2", "placeholder_title"),
        ("Test-2 dataset", "placeholder_title"),
        ("Test dataset", "placeholder_title"),
        ("My Test Dataset", "placeholder_title"),
        ("asdf", "placeholder_title"),
        ("bla", "placeholder_title"),
        ("ZZZz", "placeholder_title"),
        ("ABC", "placeholder_title"),
        ("Unnamed Dataset", "placeholder_title"),
        ("TODO: name of the dataset", "placeholder_title"),
        ("BIDS dataset", "placeholder_title"),
        ("dataset2", "placeholder_title"),
        ("Placeholder Unembargoed Dandiset", "placeholder_title"),
        ("user test", "placeholder_title"),
        ("Brandon's Test Dandiset", "title_keyword:test"),
        ("NWB API Test Data", "title_keyword:test"),
        ("Testing the Dandi", "title_keyword:testing"),
        ("ASCENT Tutorial", "title_keyword:tutorial"),
        ("Example MR artifacts", "title_keyword:example"),
    ])
    def test_junk_titles(self, title, reason):
        assert junk_reason("OpenNeuro", "ds999999", title) == reason

    @pytest.mark.parametrize("title", [
        "A test-retest fMRI dataset for motor, language and spatial attention",
        "7T resting state test-retest",
        "Maclaren test–retest brain volume dataset",
        "MC_Maze: macaque primary motor and dorsal premotor cortex spiking activity",
        "FALCON Benchmark M1-A: primary motor cortex recordings in primate",
        "LITMUS: Rat-based simulated motor unit dataset with synthetic waveforms",
        "Antibodies tested in the colon - Pig",
        "Light sheet imaging of the human brain",
        "Human vagus nerve anatomical reconstruction, sample 12",
    ])
    def test_real_titles_pass(self, title):
        assert junk_reason("DANDI", "000999", title) is None

    def test_description_keywords_no_longer_count(self):
        # The old filter skipped these because "test" / "sample" / "benchmark"
        # appeared in the description; they are real datasets.
        desc = "Recordings during a behavioral test. A tissue sample from each animal. Benchmark results in the paper."
        assert junk_reason("DANDI", "000059", "Cooling of Medial Septum Reveals Theta Phase Lag", desc) is None

    def test_description_test_upload_phrase(self):
        assert junk_reason("DANDI", "000999", "Spinal recordings", "This is a test dataset, to be deleted.") == "description_phrase"
        assert junk_reason("OpenNeuro", "ds999999", "RSVP", "TODO: Provide description for the dataset") == "description_phrase"

    def test_no_title_and_title_is_id(self):
        assert junk_reason("OpenNeuro", "ds000006", None) == "no_title"
        assert junk_reason("OpenNeuro", "ds000006", "   ") == "no_title"
        assert junk_reason("OpenNeuro", "ds000006", "ds000006") == "title_is_id"
        assert junk_reason("OpenNeuro", "ds000006", "DS000006") == "title_is_id"

    def test_find_reuse_curated_dandisets(self):
        assert "000027" in FIND_REUSE_TEST_DANDISETS
        assert junk_reason("DANDI", "000027", "A perfectly normal looking title") == "find_reuse_test_id"
        # Only DANDI ids are curated.
        assert junk_reason("OpenNeuro", "000027", "A perfectly normal looking title") is None

    def test_empty_dandiset(self):
        assert junk_reason("DANDI", "000999", "Real title", asset_count=0) == "empty"
        assert junk_reason("DANDI", "000999", "Real title", asset_count=12) is None
        assert junk_reason("DANDI", "000999", "Real title", asset_count=None) is None
        # asset_count is a DANDI concept; other archives ignore it.
        assert junk_reason("SPARC", "157", "Real title", asset_count=0) is None


class TestComputeStatus:
    def test_mapping_wins_over_junk(self):
        assert compute_status(mapped=True, papers=0, junk="placeholder_title") == (STATUS_MAPPED, None)

    def test_junk_wins_over_no_paper(self):
        assert compute_status(mapped=False, papers=0, junk="empty") == (STATUS_EXCLUDED, "empty")

    def test_no_paper_and_pending(self):
        assert compute_status(mapped=False, papers=0, junk=None) == (STATUS_NO_PAPER, None)
        assert compute_status(mapped=False, papers=None, junk=None) == (STATUS_PENDING, None)
        # papers > 0 without a map row (legacy count) is still pending: nothing to show.
        assert compute_status(mapped=False, papers=3, junk=None) == (STATUS_PENDING, None)


class TestDdl:
    def test_dandi_gets_asset_count(self):
        ddl = dataset_status_ddl("DANDI")
        assert "dandi_dataset ADD COLUMN IF NOT EXISTS asset_count INTEGER" in ddl
        assert "dandi_dataset ADD COLUMN IF NOT EXISTS dataset_status TEXT" in ddl
        assert "dandi_dataset ADD COLUMN IF NOT EXISTS dataset_status_reason TEXT" in ddl

    def test_other_archives_do_not(self):
        assert "asset_count" not in dataset_status_ddl("SPARC")
        assert "sparc_dataset ADD COLUMN IF NOT EXISTS dataset_status TEXT" in dataset_status_ddl("SPARC")


class FakeCursor:
    """Answers the schema lookups, returns one archive's rows, records UPDATEs."""

    def __init__(self, rows, map_table_exists=True, columns=None):
        self.rows = rows
        self.map_table_exists = map_table_exists
        self.columns = columns or {}
        self.updates = []
        self.executed = []
        self._result = None

    def execute(self, sql, args=()):
        s = " ".join(sql.split())
        self.executed.append(s)
        if s.startswith("SELECT to_regclass"):
            self._result = [(self.map_table_exists if "paper_map" in args[0] else True,)]
        elif "information_schema.columns" in s and "column_name = %s" in s:
            self._result = [(args[1] in self.columns.get(args[0], set()),)]
        elif s.startswith("SELECT column_name FROM information_schema.columns"):
            self._result = [(c,) for c in self.columns.get(args[0], set())]
        elif s.startswith("SELECT d.dataset_id"):
            self._result = list(self.rows)
        else:
            self._result = []

    def executemany(self, sql, seq):
        self.updates.extend(seq)

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result)


def _row(ds_id, title, papers=None, status=None, reason=None, asset_count=None, mapped=False, description=None):
    return (ds_id, title, description, papers, status, reason, asset_count, mapped)


class TestRefresh:
    def test_assigns_every_status_and_updates_only_changes(self):
        cur = FakeCursor(
            rows=[
                _row("000001", "Real mapped dataset", papers=2, mapped=True, status="mapped"),
                _row("000002", "Real, tried, nothing", papers=0),
                _row("000003", "Real, never tried"),
                _row("000777", "Test"),
                _row("000027", "Looks fine but curated out"),
                _row("000500", "Empty dandiset", asset_count=0),
                _row("000600", "Was junk, now mapped", status="excluded", reason="placeholder_title", mapped=True),
            ],
            columns={"dandi_dataset": {"dataset_status", "dataset_status_reason", "asset_count"}},
        )
        result = refresh_dataset_status(cur, "DANDI")
        assert result["by_status"] == {
            STATUS_MAPPED: 2, STATUS_NO_PAPER: 1, STATUS_PENDING: 1, STATUS_EXCLUDED: 3,
        }
        assert result["junk_reasons"] == {"placeholder_title": 1, "find_reuse_test_id": 1, "empty": 1}
        # 000001 already had its status; everything else changed.
        assert result["updated"] == 6
        by_id = {ds: (st, rs) for st, rs, ds in cur.updates}
        assert by_id["000002"] == (STATUS_NO_PAPER, None)
        assert by_id["000003"] == (STATUS_PENDING, None)
        assert by_id["000777"] == (STATUS_EXCLUDED, "placeholder_title")
        assert by_id["000600"] == (STATUS_MAPPED, None)
        assert "000001" not in by_id
        # The mapped flag came from the paper map, the asset count from the column.
        select = next(s for s in cur.executed if s.startswith("SELECT d.dataset_id"))
        assert "EXISTS (SELECT 1 FROM dandi_paper_map m WHERE m.dandi_id = d.dataset_id)" in select
        assert "d.asset_count AS asset_count" in select

    def test_without_a_paper_map_nothing_is_mapped(self):
        cur = FakeCursor(rows=[_row("157", "Real title", papers=None)], map_table_exists=False)
        refresh_dataset_status(cur, "SPARC")
        select = next(s for s in cur.executed if s.startswith("SELECT d.dataset_id"))
        assert "FALSE AS mapped" in select
        assert "NULL AS asset_count" in select
        assert cur.updates == [(STATUS_PENDING, None, "157")]
