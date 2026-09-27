"""CRCNS publication dates: a real day when DataCite supports one, otherwise the year alone."""

from datetime import datetime

import pytest

pytest.importorskip("airflow")

import crcns_ingestion as CI  # noqa: E402


class TestPublishedDate:
    def test_registration_in_the_publication_year_gives_the_day(self):
        attrs = {"publicationYear": 2015, "registered": "2015-06-24T18:37:33.000Z"}
        assert CI._published_date(attrs) == (datetime(2015, 6, 24, 18, 37, 33), "day")

    def test_a_doi_registered_later_keeps_only_the_year(self):
        # CRCNS aa-2 (10.6080/K0JW8BSC): published 2011, DOI registered in 2013.
        attrs = {
            "publicationYear": 2011,
            "registered": "2013-06-25T23:37:09.000Z",
            "dates": [{"date": "2011", "dateType": "Issued"}],
        }
        assert CI._published_date(attrs) == (datetime(2011, 1, 1), "year")

    def test_a_created_date_wins(self):
        attrs = {
            "publicationYear": 2011,
            "registered": "2011-09-01T00:00:00Z",
            "dates": [{"date": "2010-03-04", "dateType": "Created"}],
        }
        assert CI._published_date(attrs) == (datetime(2010, 3, 4), "day")

    def test_year_as_text_and_no_registration(self):
        assert CI._published_date({"publicationYear": "2012"}) == (datetime(2012, 1, 1), "year")

    def test_nothing_known(self):
        assert CI._published_date({}) == (None, None)
        assert CI._published_date({"registered": "2015-06-24T18:37:33Z"}) == (None, None)
