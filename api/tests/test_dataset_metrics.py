"""
Tests for the dataset reuse metrics: build_reuse_metrics, the author-name
matching behind independent reuse, and the route order that keeps "/metrics"
from being read as part of a dataset id.

No database: the builder takes plain edge rows.
"""

import asyncio

import pytest
from fastapi import HTTPException
from starlette.routing import Match

import main as M


def edge(doi, label=None, *, status=None, same_lab=None, date=None, text_status="full_text",
         authors=None, title=None):
    return {
        "citing_paper_doi": doi,
        "classification": label,
        "status": status or ("classified" if label else None),
        "same_lab": same_lab,
        "publication_date": date,
        "publication_year": int(date[:4]) if date else None,
        "text_status": text_status,
        "title": title,
        "authors": authors,
    }


def metrics(edges, dataset_authors=(), primary_papers=(), published_year=2021, current_year=2026):
    return M.build_reuse_metrics(
        list(edges), dataset_authors=list(dataset_authors), primary_papers=list(primary_papers),
        published_year=published_year, current_year=current_year,
    )


class TestAuthorKey:
    def test_comma_and_natural_order_match(self):
        assert M._author_key("Cavanagh, James F") == ("cavanagh", "j")
        assert M._author_key("James F. Cavanagh") == ("cavanagh", "j")

    def test_accents_and_invisible_characters_are_dropped(self):
        assert M._author_key("\u202cSiamak Shahidi") == ("shahidi", "s")
        assert M._author_key("Frömer, Romy") == M._author_key("Romy Frömer") == ("fromer", "r")

    def test_compound_and_hyphenated_surnames(self):
        assert M._author_key("Pirio-Richardson, Sarah") == ("richardson", "s")
        assert M._author_key("Sarah Pirio Richardson") == ("richardson", "s")
        assert M._author_key("Thi\u2010Nhu\u2010Quynh Nguyen") == ("nguyen", "t")

    def test_suffix_is_ignored(self):
        assert M._author_key("John Smith Jr.") == ("smith", "j")

    def test_contributor_records_use_their_name(self):
        assert M._author_key({"name": "Churchland, Mark", "roles": ["dcite:Author"]}) == ("churchland", "m")

    @pytest.mark.parametrize("name", ["Cavanagh", "", "  ", None, 42])
    def test_unusable_names(self, name):
        assert M._author_key(name) is None


class TestBuildReuseMetrics:
    def test_counts_distinct_citing_papers_across_primary_papers(self):
        m = metrics([edge("10.1/a", "REUSE"), edge("10.1/a", "MENTION"), edge("10.1/b", "MENTION")])
        assert m["reuse_count"] == 1
        assert m["mention_count"] == 1
        assert m["coverage"]["citing_papers"] == 2

    def test_label_precedence(self):
        m = metrics([
            edge("10.1/mention", "NEITHER"), edge("10.1/mention", "MENTION"),
            edge("10.1/primary", "MENTION"), edge("10.1/primary", "PRIMARY"),
        ])
        assert m["mention_count"] == 1
        assert m["reuse_count"] == 0
        assert m["coverage"]["classified"] == 2

    def test_classifier_same_lab(self):
        m = metrics([edge("10.1/a", "REUSE", same_lab=True, authors=["Ann Other"])], dataset_authors=["Cavanagh, James F"])
        assert (m["independent_reuse_count"], m["same_lab_reuse_count"]) == (0, 1)
        assert m["reuse_papers"][0]["same_lab_basis"] == ["classifier"]

    def test_author_name_overlap_counts_as_same_lab_when_the_classifier_says_no(self):
        m = metrics(
            [edge("10.1/a", "REUSE", same_lab=False, authors=["Ann Other", "James F. Cavanagh"])],
            dataset_authors=["Cavanagh, James F"],
        )
        assert m["same_lab_reuse_count"] == 1
        assert m["reuse_papers"][0]["same_lab_basis"] == ["author_names"]

    def test_independent_when_neither_signal(self):
        m = metrics(
            [edge("10.1/a", "REUSE", same_lab=None, authors=["Ann Other"])],
            dataset_authors=["Cavanagh, James F"],
        )
        assert (m["independent_reuse_count"], m["same_lab_reuse_count"]) == (1, 0)
        assert m["reuse_papers"][0] == {
            "doi": "10.1/a", "title": None, "first_author": "Ann Other", "author_count": 1,
            "publication_date": None, "first_date": None, "versions": [],
            "same_lab": False, "same_lab_basis": [],
        }

    def test_per_year_runs_from_publication_to_the_current_year(self):
        m = metrics(
            [edge("10.1/a", "REUSE", date="2022-12-14"), edge("10.1/b", "MENTION", date="2023-04-20")],
            published_year=2021, current_year=2026,
        )
        assert m["per_year"] == [
            {"year": 2021, "reuse": 0, "mentions": 0},
            {"year": 2022, "reuse": 1, "mentions": 0},
            {"year": 2023, "reuse": 0, "mentions": 1},
            {"year": 2024, "reuse": 0, "mentions": 0},
            {"year": 2025, "reuse": 0, "mentions": 0},
            {"year": 2026, "reuse": 0, "mentions": 0},
        ]

    def test_papers_dated_before_publication_extend_the_range(self):
        m = metrics([edge("10.1/a", "MENTION", date="2019-03")], published_year=2021, current_year=2022)
        assert [y["year"] for y in m["per_year"]] == [2019, 2020, 2021, 2022]

    def test_undated_papers_are_counted_separately(self):
        m = metrics([edge("10.1/a", "REUSE"), edge("10.1/b", "MENTION"), edge("10.1/c", "MENTION")])
        assert m["undated"] == {"reuse": 1, "mentions": 2}
        assert all(y["reuse"] == 0 and y["mentions"] == 0 for y in m["per_year"])

    def test_last_reuse_is_the_latest_dated_reuse(self):
        m = metrics([
            edge("10.1/old", "REUSE", date="2022-12-14", authors=["Thi Nguyen"]),
            edge("10.1/new", "REUSE", date="2023-11-24", authors=["Sajjad Farashi"], title="Tremor"),
            edge("10.1/undated", "REUSE"),
        ])
        assert m["last_reuse"]["doi"] == "10.1/new"
        assert m["last_reuse"]["first_author"] == "Sajjad Farashi"
        assert [p["doi"] for p in m["reuse_papers"]] == ["10.1/new", "10.1/old", "10.1/undated"]

    def test_coverage_buckets(self):
        m = metrics([
            edge("10.1/labelled", "MENTION"),
            edge("10.1/classifier-no-text", status="no_full_text"),
            edge("10.1/mapping-no-text", text_status="metadata_only"),
            edge("10.1/unavailable", text_status="unavailable"),
            edge("10.1/failed", status="error"),
            edge("10.1/not-yet"),
        ])
        assert m["coverage"] == {"citing_papers": 6, "classified": 1, "no_full_text": 3, "pending": 2}

    def test_no_edges(self):
        m = metrics([], published_year=2024, current_year=2026)
        assert (m["reuse_count"], m["independent_reuse_count"], m["mention_count"]) == (0, 0, 0)
        assert m["last_reuse"] is None
        assert [y["year"] for y in m["per_year"]] == [2024, 2025, 2026]
        assert m["coverage"] == {"citing_papers": 0, "classified": 0, "no_full_text": 0, "pending": 0}

    def test_no_publication_date_and_no_data_gives_no_years(self):
        assert metrics([], published_year=None)["per_year"] == []


class TestSameLabByAuthorIds:
    """OpenAlex author ids decide against the primary papers when both sides have them."""

    PRIMARY = [{"authors": ["Li Wang", "James F. Cavanagh"], "author_ids": ["A100", "A200"]}]

    def reuse(self, authors, author_ids=None):
        return [edge("10.1/r", "REUSE", same_lab=False, authors=authors, **({"author_ids": author_ids} if author_ids else {}))]

    def basis(self, rows, dataset_authors=()):
        m = metrics(rows, dataset_authors=dataset_authors, primary_papers=self.PRIMARY)
        return m["reuse_papers"][0]["same_lab_basis"], m["independent_reuse_count"]

    def test_a_shared_id_is_same_lab_even_when_the_name_is_written_differently(self):
        rows = [{**r, "author_ids": ["A999", "A200"]} for r in self.reuse(["J. F. Cavanaugh"])]
        assert self.basis(rows) == (["author_ids"], 0)

    def test_a_common_name_with_a_different_id_is_independent(self):
        # "L. Wang" matches "Li Wang" by name, but OpenAlex says it is someone else.
        rows = [{**r, "author_ids": ["A777"]} for r in self.reuse(["L. Wang"])]
        assert self.basis(rows) == ([], 1)

    def test_names_decide_when_the_citing_paper_has_no_ids(self):
        assert self.basis(self.reuse(["L. Wang"])) == (["author_names"], 0)

    def test_archive_authors_still_match_by_name(self):
        # The archive's author list has no ids, so a name match there counts.
        rows = [{**r, "author_ids": ["A777"]} for r in self.reuse(["Arun Singh"])]
        assert self.basis(rows, dataset_authors=["Singh, Arun"]) == (["author_names"], 0)


class TestVersionsCountOnce:
    """A preprint and its published version share a work key and are one paper."""

    PREPRINT = "10.1101/2023.05.08.539865"
    PUBLISHED = "10.1111/psyp.14478"

    def versions(self, preprint_label=None, published_label="REUSE", **published):
        return [
            edge(self.PREPRINT, preprint_label, date="2023-05-10", text_status="metadata_only",
                 authors=["Daniel J. McKeown"] if preprint_label == "REUSE" else None),
            edge(self.PUBLISHED, published_label, date="2023-11-08",
                 authors=["Daniel J. McKeown", "James F. Cavanagh"], **published),
        ]

    def keyed(self, rows, key="t:medicationinvariantrestingaperiodic"):
        return [{**r, "work_key": key} for r in rows]

    def test_counted_once_under_the_strongest_label(self):
        m = metrics(self.keyed(self.versions()))
        assert (m["reuse_count"], m["mention_count"]) == (1, 0)
        assert m["coverage"] == {"citing_papers": 1, "classified": 1, "no_full_text": 0, "pending": 0}

    def test_dated_by_the_earliest_version_and_shown_as_the_published_one(self):
        m = metrics(self.keyed(self.versions()), current_year=2024)
        paper = m["reuse_papers"][0]
        assert (paper["doi"], paper["publication_date"], paper["first_date"]) == (self.PUBLISHED, "2023-11-08", "2023-05-10")
        assert paper["versions"] == [
            {"doi": self.PREPRINT, "is_preprint": True, "publication_date": "2023-05-10"},
            {"doi": self.PUBLISHED, "is_preprint": False, "publication_date": "2023-11-08"},
        ]
        assert m["last_reuse"]["first_date"] == "2023-05-10"
        assert [(y["year"], y["reuse"]) for y in m["per_year"] if y["reuse"]] == [(2023, 1)]

    def test_label_from_either_version(self):
        m = metrics(self.keyed(self.versions(preprint_label="REUSE", published_label=None)))
        assert m["reuse_count"] == 1
        assert m["reuse_papers"][0]["doi"] == self.PUBLISHED
        assert m["reuse_papers"][0]["first_author"] == "Daniel J. McKeown"

    def test_same_lab_if_any_version_is(self):
        m = metrics(self.keyed(self.versions(same_lab=True)), dataset_authors=["Someone Else"])
        assert m["reuse_papers"][0]["same_lab_basis"] == ["classifier"]

    def test_without_a_shared_key_they_stay_two_papers(self):
        m = metrics(self.versions(preprint_label="MENTION"))
        assert (m["reuse_count"], m["mention_count"], m["coverage"]["citing_papers"]) == (1, 1, 2)

    def test_the_work_is_no_full_text_only_if_every_version_is(self):
        rows = self.keyed([
            edge(self.PREPRINT, status="no_full_text"),
            edge(self.PUBLISHED, text_status="full_text"),
        ])
        assert metrics(rows)["coverage"] == {"citing_papers": 1, "classified": 0, "no_full_text": 0, "pending": 1}


class TestPreprintsAndVersions:
    @pytest.mark.parametrize("doi", [
        "10.1101/2023.05.08.539865", "10.1101/430858", "10.64898/2026.02.20.707132",
        "10.48550/arXiv.2301.00001", "10.21203/rs.3.rs-123/v1", "10.2139/ssrn.4012345", "10.31234/osf.io/abcd",
    ])
    def test_preprint_servers(self, doi):
        assert M.is_preprint_doi(doi)

    @pytest.mark.parametrize("doi", [
        "10.1101/gr.275648.121",  # Genome Research: Cold Spring Harbor, same prefix as bioRxiv
        "10.1111/psyp.14478", "10.7554/eLife.84630", None,
    ])
    def test_journals_are_not_preprints(self, doi):
        assert not M.is_preprint_doi(doi)

    def test_published_version_over_the_preprint(self):
        assert M.pick_published_version(["10.1101/2023.05.08.539865", "10.1111/psyp.14478"]) == "10.1111/psyp.14478"

    def test_elife_umbrella_over_numbered_versions(self):
        dois = ["10.7554/eLife.95127.3", "10.7554/eLife.95127", "10.7554/eLife.95127.1"]
        assert M.pick_published_version(dois) == "10.7554/eLife.95127"

    def test_annotate_versions_marks_each_row(self):
        rows = [
            {"citing_paper_doi": "10.1101/2023.05.08.539865", "citing_work_key": "t:x", "citing_publication_date": "2023-05-10"},
            {"citing_paper_doi": "10.1111/psyp.14478", "citing_work_key": "t:x", "citing_publication_date": "2023-11-08"},
            {"citing_paper_doi": "10.1/solo", "citing_work_key": None, "citing_publication_date": None},
        ]
        M._annotate_versions(rows, doi_key="citing_paper_doi", work_key="citing_work_key",
                             date_key="citing_publication_date", prefix="citing_")
        assert [r["citing_work_doi"] for r in rows] == ["10.1111/psyp.14478", "10.1111/psyp.14478", "10.1/solo"]
        assert [r["citing_is_preprint"] for r in rows] == [True, False, False]
        assert rows[0]["citing_work_versions"] == rows[1]["citing_work_versions"] == [
            {"doi": "10.1101/2023.05.08.539865", "is_preprint": True, "publication_date": "2023-05-10"},
            {"doi": "10.1111/psyp.14478", "is_preprint": False, "publication_date": "2023-11-08"},
        ]
        assert rows[2]["citing_work_versions"] == [] and rows[2]["citing_work_key"] == "d:10.1/solo"

    def test_work_key_sql_uses_the_title_and_falls_back_to_the_doi(self):
        expr = M.work_key_sql("p.title", "c.citing_paper_doi")
        assert "COALESCE(p.title, '')" in expr and "lower(c.citing_paper_doi)" in expr
        assert f">= {M.WORK_KEY_MIN_CHARS}" in expr


class TestStagingExample:
    """OpenNeuro ds003509 (SimonConflict) as staging had it on 2026-09-26: the mockups' numbers."""

    DATASET_AUTHORS = ("James F Cavanagh", "Arun Singh", "Kumar Narayanan")
    PRIMARY_PAPERS = (
        {"authors": ["James F. Cavanagh", "Andrea A. Mueller", "Darin R. Brown", "Jacqueline R. Janowich",
                     "Jacqueline H. Story-Remer", "Ashley Wegele", "Sarah Pirio Richardson"]},  # Cortex 2017
        {"authors": ["James F. Cavanagh", "Sean E. Masters", "Kevin Bath", "Michael J. Frank"]},  # Nat Commun 2014
    )
    MENTION_DATES = (
        "2021-06-10", "2021-08-27", "2021-12-10", "2022-02-10", "2022-09-26", "2023-04-20",
        "2023-05-19", "2024-10-08", "2025-01-16", "2025-05-30", "2025-11-01",
    )
    NO_TEXT_DATES = ("2021-06-29", "2022-01-01", "2023-01-18", "2023-05-10", "2023-07-10", "2024-12-19")

    def edges(self):
        rows = [
            edge("10.1007/978-3-031-19694-2_10", "REUSE", same_lab=False, date="2022-12-14",
                 authors=["Thi\u2010Nhu\u2010Quynh Nguyen", "Hoang\u2010Thuy\u2010Tien Vo", "Huy A. Nguyen", "Tuan Van Huynh"]),
            edge("10.1111/psyp.14478", "REUSE", same_lab=True, date="2023-11-08",
                 authors=["Daniel J. McKeown", "Manon Jones", "Camilla Pihl", "James F. Cavanagh", "Douglas J. Angus"]),
            edge("10.1186/s12883-023-03468-0", "REUSE", same_lab=False, date="2023-11-24",
                 authors=["Sajjad Farashi", "Abdolrahman Sarihi", "Mahdi Ramezani", "\u202cSiamak Shahidi", "Mehrdokht Mazdeh"]),
        ]
        rows += [edge(f"10.1/mention-{i}", "MENTION", date=d) for i, d in enumerate(self.MENTION_DATES)]
        rows += [edge(f"10.1/no-text-{i}", date=d, text_status="metadata_only") for i, d in enumerate(self.NO_TEXT_DATES)]
        return rows

    def test_headline_numbers(self):
        m = metrics(self.edges(), dataset_authors=self.DATASET_AUTHORS, primary_papers=self.PRIMARY_PAPERS, published_year=2021, current_year=2026)
        assert m["reuse_count"] == 3
        assert m["independent_reuse_count"] == 2
        assert m["same_lab_reuse_count"] == 1
        assert m["mention_count"] == 11
        assert m["last_reuse"]["doi"] == "10.1186/s12883-023-03468-0"
        assert (m["last_reuse"]["first_author"], m["last_reuse"]["author_count"]) == ("Sajjad Farashi", 5)
        assert m["coverage"] == {"citing_papers": 20, "classified": 14, "no_full_text": 6, "pending": 0}

    def test_same_lab_reuse_is_backed_by_both_signals(self):
        m = metrics(self.edges(), dataset_authors=self.DATASET_AUTHORS, primary_papers=self.PRIMARY_PAPERS)
        same_lab = [p for p in m["reuse_papers"] if p["same_lab"]]
        assert [(p["doi"], p["same_lab_basis"]) for p in same_lab] == [
            ("10.1111/psyp.14478", ["classifier", "author_names"])
        ]

    def test_per_year(self):
        m = metrics(self.edges(), dataset_authors=self.DATASET_AUTHORS, primary_papers=self.PRIMARY_PAPERS, published_year=2021, current_year=2026)
        assert [(y["year"], y["reuse"], y["mentions"]) for y in m["per_year"]] == [
            (2021, 0, 3), (2022, 1, 2), (2023, 2, 2), (2024, 0, 1), (2025, 0, 3), (2026, 0, 0),
        ]


def _route_for(path):
    scope = {"type": "http", "path": path, "method": "GET", "root_path": ""}
    for route in M.app.router.routes:
        match, child_scope = route.matches(scope)
        if match == Match.FULL:
            return route.endpoint, child_scope["path_params"]
    return None, None


class TestRoutes:
    def test_metrics_path_reaches_the_metrics_route_even_for_ids_with_slashes(self):
        endpoint, params = _route_for("/api/datasets/crcns/10.6080/k0s46pv7/metrics")
        assert endpoint is M.get_dataset_metrics
        assert params == {"source": "crcns", "dataset_id": "10.6080/k0s46pv7"}

    def test_detail_path_still_reaches_the_detail_route(self):
        endpoint, params = _route_for("/api/datasets/crcns/10.6080/k0s46pv7")
        assert endpoint is M.get_dataset_detail
        assert params["dataset_id"] == "10.6080/k0s46pv7"

    def test_dropped_seed_sources_are_not_found(self):
        # Kaggle and PhysioNet seed rows are no longer exposed anywhere on the site.
        for source in ("kaggle", "physionet"):
            with pytest.raises(HTTPException) as err:
                asyncio.run(M.get_dataset_metrics(source, "UCI/epileptic-seizure"))
            assert err.value.status_code == 404

    def test_unknown_archive_is_not_found(self):
        with pytest.raises(HTTPException) as err:
            asyncio.run(M.get_dataset_metrics("figshare", "123"))
        assert err.value.status_code == 404
