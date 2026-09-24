"""
Tests for two dataset-identity fixes:

- CRCNS: short-code extraction from the doi.org landing URL, and re-keying
  datasets that were stored under their DOI before the code could be parsed.
- SPARC: linked DOIs that cannot be a dataset's primary paper (SPARC dataset
  DOIs, books, correction notices) are dropped.

Importing the DAG modules needs Airflow (present in the containers). No
network and no database: the re-key test uses a fake cursor.
"""

import psycopg2
import pytest

pytest.importorskip("airflow")

import crcns_ingestion as CI  # noqa: E402
import sparc_paper_mapping as SP  # noqa: E402
from utils.find_reuse_core import is_definitive_no_paper  # noqa: E402


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

    def __init__(self, tables, unique=()):
        self.tables = tables
        self.unique = set(unique)  # tables whose UPDATE should collide
        self.rowcount = 0
        self._result = None
        self._saved = None

    def execute(self, sql, args=()):
        s = " ".join(sql.split())
        if s.startswith("SAVEPOINT"):
            self._saved = {k: list(v) for k, v in self.tables.items()}
        elif s.startswith("RELEASE SAVEPOINT"):
            self._saved = None
        elif s.startswith("ROLLBACK TO SAVEPOINT"):
            self.tables = self._saved
        elif s.startswith("SELECT to_regclass"):
            name = args[0].split(".", 1)[1]
            self._result = (name in self.tables,)
        elif s.startswith("SELECT 1 FROM crcns_dataset"):
            self._result = (1,) if args[0] in self.tables["crcns_dataset"] else None
        elif s.startswith("UPDATE"):
            table = s.split()[1]
            new, old = args
            ids = self.tables[table]
            if table in self.unique and new in ids and old in ids:
                raise psycopg2.IntegrityError("duplicate key value violates unique constraint")
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

    def test_conflict_in_a_paper_table_rolls_back_and_does_not_raise(self):
        # Map rows exist under both ids: the UPDATE would violate UNIQUE. The
        # dataset row must stay on the DOI too (all-or-nothing), and ingestion goes on.
        cur = FakeCursor({
            "crcns_dataset": [self.DOI],
            "crcns_paper_map": [self.DOI, self.CODE],
            "crcns_paper_citations": [],
            "crcns_paper_citation_classifications": [],
        }, unique={"crcns_paper_map"})
        assert CI._rekey_doi_rows(cur, self.DOI, self.CODE) == 0
        assert cur.tables["crcns_dataset"] == [self.DOI]
        assert cur.tables["crcns_paper_map"] == [self.DOI, self.CODE]

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

    def test_new_version_is_still_a_paper(self):
        # Only correction-type updates are notices; a new version is a real paper.
        assert SP._non_primary_reason("10.1/x", {"type": "journal-article", "update_types": ["new_version"]}) is None

    def test_correction_notice(self):
        reason = SP._non_primary_reason("10.1007/s00441-019-03040-8",
                                        {"type": "journal-article", "update_types": ["correction"]})
        assert reason == "update_notice:correction"

    def test_missing_crossref_metadata_keeps_the_doi(self):
        # Crossref down or unknown DOI: no evidence against it, keep it.
        assert SP._non_primary_reason("10.1234/x", {"title": None}) is None


class TestDefinitiveNoPaper:
    @pytest.mark.parametrize("reason", [
        "no_dois_found", "no_papers_found", "no_external_publications",
        "no_primary_papers_after_filtering", "pennsieve_404", "http_404",
    ])
    def test_real_no_paper_outcomes(self, reason):
        assert is_definitive_no_paper(reason)

    @pytest.mark.parametrize("reason", [
        # A failed attempt must leave papers unset so the next run retries it,
        # e.g. an OpenAlex quota refusal surfaces as reason=exception.
        "exception", "missing_in_db", "dandi_metadata_unavailable", "openneuro_metadata_unavailable",
        "pennsieve_unreachable", "pennsieve_http_503", "pennsieve_invalid_json",
        "request_error:ReadTimeout", "server_error_502", "http_429", None, "",
    ])
    def test_failed_attempts_are_not(self, reason):
        assert not is_definitive_no_paper(reason)
