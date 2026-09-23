"""
Tests for two dataset-identity fixes:

- CRCNS: short-code extraction from the doi.org landing URL, and re-keying
  datasets that were stored under their DOI before the code could be parsed.
- SPARC: linked DOIs that cannot be a dataset's primary paper (SPARC dataset
  DOIs, books, correction notices) are dropped.

Importing the DAG modules needs Airflow (present in the containers). No
network and no database: the re-key test uses a fake cursor.
"""

import pytest

pytest.importorskip("airflow")

import crcns_ingestion as CI  # noqa: E402
import sparc_paper_mapping as SP  # noqa: E402


class TestCrcnsCode:
    @pytest.mark.parametrize("url,code", [
        ("https://crcns.org/data-sets/motor-cortex/alm-1", "alm-1"),
        ("https://crcns.org/data-sets/motor-cortex/pmd-1", "pmd-1"),
        ("https://crcns.org/data-sets/challenges/ch-epfl-2009", "ch-epfl-2009"),
        ("https://crcns.org/data-sets/ssc/ssc-1/about-ssc-1", "ssc-1"),
        ("https://crcns.org/data-sets/hc/hc-3", "hc-3"),
        ("https://crcns.org/data-sets/vc/pvc-11", "pvc-11"),
        ("https://crcns.org/data-sets/methods/cai-2", "cai-2"),
    ])
    def test_code_is_parsed(self, url, code):
        assert CI._CRCNS_CODE_RE.search(url).group(1) == code

    def test_ezid_landing_page_has_no_code(self):
        # DataCite points a few CRCNS DOIs at a dead EZID page; those keep the DOI.
        assert CI._CRCNS_CODE_RE.search("https://ezid.cdlib.org:443/id/doi:10.6080/K03F4MH2") is None


class FakeCursor:
    """Just enough of a cursor for _rekey_doi_rows: a dict of table -> list of ids."""

    def __init__(self, tables):
        self.tables = tables
        self.rowcount = 0
        self._result = None

    def execute(self, sql, args=()):
        s = " ".join(sql.split())
        if s.startswith("SELECT to_regclass"):
            name = args[0].split(".", 1)[1]
            self._result = (name in self.tables,)
        elif s.startswith("SELECT 1 FROM crcns_dataset"):
            self._result = (1,) if args[0] in self.tables["crcns_dataset"] else None
        elif s.startswith("UPDATE"):
            table = s.split()[1]
            new, old = args
            ids = self.tables[table]
            self.rowcount = sum(1 for i in ids if i == old)
            self.tables[table] = [new if i == old else i for i in ids]
        else:
            raise AssertionError(f"unexpected SQL: {s}")

    def fetchone(self):
        return self._result


class TestRekey:
    DOI, CODE = "10.6080/k0ms3qnt", "alm-1"

    def test_moves_every_table(self):
        cur = FakeCursor({
            "crcns_dataset": [self.DOI, "ssc-1"],
            "crcns_paper_map": [self.DOI, "ssc-1"],
            "crcns_paper_citations": [self.DOI, self.DOI, "ssc-1"],
            "crcns_paper_citation_classifications": [],
        })
        assert CI._rekey_doi_rows(cur, self.DOI, self.CODE) == 4
        assert cur.tables["crcns_dataset"] == [self.CODE, "ssc-1"]
        assert cur.tables["crcns_paper_citations"] == [self.CODE, self.CODE, "ssc-1"]

    def test_noop_when_nothing_is_keyed_by_the_doi(self):
        cur = FakeCursor({"crcns_dataset": [self.CODE], "crcns_paper_map": [self.CODE]})
        assert CI._rekey_doi_rows(cur, self.DOI, self.CODE) == 0
        assert cur.tables["crcns_dataset"] == [self.CODE]

    def test_refuses_when_both_ids_exist(self):
        cur = FakeCursor({"crcns_dataset": [self.DOI, self.CODE], "crcns_paper_map": [self.DOI]})
        assert CI._rekey_doi_rows(cur, self.DOI, self.CODE) == 0
        assert cur.tables["crcns_paper_map"] == [self.DOI]

    def test_skips_paper_tables_that_do_not_exist_yet(self):
        cur = FakeCursor({"crcns_dataset": [self.DOI]})
        assert CI._rekey_doi_rows(cur, self.DOI, self.CODE) == 1


class TestSparcNonPrimary:
    def test_real_article_is_kept(self):
        assert SP._non_primary_reason("10.1113/jp273259", {"type": "journal-article", "update_types": []}) is None

    def test_sparc_dataset_doi(self):
        assert SP._non_primary_reason("10.26275/k0mx-jcth", {}) == "sparc_dataset_doi"

    def test_book(self):
        # Spiking Neuron Models, linked by SPARC dataset 157 as `References`.
        assert SP._non_primary_reason("10.1017/cbo9780511815706", {"type": "monograph"}) == "crossref_type:monograph"

    def test_correction_notice(self):
        reason = SP._non_primary_reason("10.1007/s00441-019-03040-8",
                                        {"type": "journal-article", "update_types": ["correction"]})
        assert reason == "update_notice:correction"

    def test_missing_crossref_metadata_keeps_the_doi(self):
        # Crossref down or unknown DOI: no evidence against it, keep it.
        assert SP._non_primary_reason("10.1234/x", {"title": None}) is None
