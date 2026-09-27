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


def metrics(edges, lab_names=(), published_year=2021, current_year=2026):
    return M.build_reuse_metrics(
        list(edges), lab_names=list(lab_names), published_year=published_year, current_year=current_year
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
        m = metrics([edge("10.1/a", "REUSE", same_lab=True, authors=["Ann Other"])], lab_names=["Cavanagh, James F"])
        assert (m["independent_reuse_count"], m["same_lab_reuse_count"]) == (0, 1)
        assert m["reuse_papers"][0]["same_lab_basis"] == ["classifier"]

    def test_author_name_overlap_counts_as_same_lab_when_the_classifier_says_no(self):
        m = metrics(
            [edge("10.1/a", "REUSE", same_lab=False, authors=["Ann Other", "James F. Cavanagh"])],
            lab_names=["Cavanagh, James F"],
        )
        assert m["same_lab_reuse_count"] == 1
        assert m["reuse_papers"][0]["same_lab_basis"] == ["author_names"]

    def test_independent_when_neither_signal(self):
        m = metrics(
            [edge("10.1/a", "REUSE", same_lab=None, authors=["Ann Other"])],
            lab_names=["Cavanagh, James F"],
        )
        assert (m["independent_reuse_count"], m["same_lab_reuse_count"]) == (1, 0)
        assert m["reuse_papers"][0] == {
            "doi": "10.1/a", "title": None, "first_author": "Ann Other", "author_count": 1,
            "publication_date": None, "same_lab": False, "same_lab_basis": [],
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


class TestStagingExample:
    """OpenNeuro ds003509 (SimonConflict) as staging had it on 2026-09-26: the mockups' numbers."""

    LAB = (
        "James F Cavanagh", "Arun Singh", "Kumar Narayanan",  # dataset
        "James F. Cavanagh", "Andrea A. Mueller", "Darin R. Brown", "Jacqueline R. Janowich",
        "Jacqueline H. Story-Remer", "Ashley Wegele", "Sarah Pirio Richardson",  # Cortex 2017
        "James F. Cavanagh", "Sean E. Masters", "Kevin Bath", "Michael J. Frank",  # Nat Commun 2014
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
        m = metrics(self.edges(), lab_names=self.LAB, published_year=2021, current_year=2026)
        assert m["reuse_count"] == 3
        assert m["independent_reuse_count"] == 2
        assert m["same_lab_reuse_count"] == 1
        assert m["mention_count"] == 11
        assert m["last_reuse"]["doi"] == "10.1186/s12883-023-03468-0"
        assert (m["last_reuse"]["first_author"], m["last_reuse"]["author_count"]) == ("Sajjad Farashi", 5)
        assert m["coverage"] == {"citing_papers": 20, "classified": 14, "no_full_text": 6, "pending": 0}

    def test_same_lab_reuse_is_backed_by_both_signals(self):
        m = metrics(self.edges(), lab_names=self.LAB)
        same_lab = [p for p in m["reuse_papers"] if p["same_lab"]]
        assert [(p["doi"], p["same_lab_basis"]) for p in same_lab] == [
            ("10.1111/psyp.14478", ["classifier", "author_names"])
        ]

    def test_per_year(self):
        m = metrics(self.edges(), lab_names=self.LAB, published_year=2021, current_year=2026)
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

    def test_archive_without_paper_mapping_is_untracked(self):
        result = asyncio.run(M.get_dataset_metrics("kaggle", "UCI/epileptic-seizure"))
        assert result == {"source": "Kaggle", "dataset_id": "UCI/epileptic-seizure", "tracked": False}

    def test_unknown_archive_is_not_found(self):
        with pytest.raises(HTTPException) as err:
            asyncio.run(M.get_dataset_metrics("figshare", "123"))
        assert err.value.status_code == 404
