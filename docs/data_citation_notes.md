# Data Citation Notes

## From the Data Citation Corpus

The [Data Citation Corpus (DCC)](https://makedatacount.org/find-a-tool/#section-1) ([data download page on Zenodo](https://zenodo.org/records/16901115), [data dashboard](https://corpus.datacite.org/dashboard)) provides structured JSON (or CSV-based) citations for datasets from various repositories, linked to their associated publications.

Here are some examples of dataset citations from different repositories, extracted from the first file in the 2025-08-15 release: `2025-08-15-data-citation-corpus-01-v4.1.json`:

Example DANDI citation:
```json
{
    "id": "00ad2451-ac70-4219-be37-99a86ad348ae",
    "created": "2025-07-25T14:30:24.335+00:00",
    "updated": "2025-07-25T14:30:24.335+00:00",
    "repository": {
      "title": "DANDI Archive",
      "external_id": null
    },
    "publisher": {
      "title": "The Royal Society",
      "external_id": null
    },
    "journal": {
      "title": "Journal of The Royal Society Interface",
      "external_id": null
    },
    "title": "Perivascular Pumping of Cerebrospinal Fluid in the Brain with a Valve Mechanism",
    "dataset": "https://doi.org/10.48324/dandi.000547/0.230822.1616",
    "publication": "https://doi.org/10.1098/rsif.2023.0288",
    "publishedDate": "2023-09-01T00:00:00+00:00",
    "source": "eupmc",
    "affiliations": [
      {
        "title": "University of Rochester",
        "external_id": "https://ror.org/022kthw22",
        "ror_name": "University of Rochester",
        "ror_id": "https://ror.org/022kthw22"
      },
      {
        "title": "University of Copenhagen",
        "external_id": "https://ror.org/035b05819",
        "ror_name": "University of Copenhagen",
        "ror_id": "https://ror.org/035b05819"
      }
    ],
    "funders": [],
    "subjects": []
}
```

The dataset DOI is prefixed with a URL to the DANDI DOI resolver.
The repository also has title "DANDI Archive".

Example OpenNeuro citation:
```json
{
    "id": "0010e150-fda9-4b6b-9414-87bd4c05bdd7",
    "created": "2025-07-24T14:09:22.555+00:00",
    "updated": "2025-08-13T17:48:48.35701+00:00",
    "repository": {
      "title": "Openneuro",
      "external_id": null
    },
    "publisher": {
      "title": "Proceedings of the National Academy of Sciences",
      "external_id": null
    },
    "journal": {
      "title": "Proceedings of the National Academy of Sciences",
      "external_id": null
    },
    "title": "Deception Signaling Task",
    "dataset": "https://doi.org/10.18112/openneuro.ds005128.v1.0.0",
    "publication": "https://doi.org/10.1073/pnas.2412881121",
    "publishedDate": "2024-12-06T00:00:00+00:00",
    "source": "eupmc",
    "affiliations": [],
    "funders": [],
    "subjects": []
}
```

Example Kaggle citation:
```json
  {
    "id": "0176dfa2-ad11-465e-b0f3-f71207128a87",
    "created": "2025-07-24T16:06:29.205+00:00",
    "updated": "2025-08-13T18:03:59.71124+00:00",
    "repository": {
      "title": "Kaggle",
      "external_id": null
    },
    "publisher": {
      "title": "Elsevier BV",
      "external_id": null
    },
    "journal": {
      "title": "Computers in Biology and Medicine",
      "external_id": null
    },
    "title": "Brain Tumor Classification (MRI)",
    "dataset": "https://doi.org/10.34740/kaggle/dsv/1183165",
    "publication": "https://doi.org/10.1016/j.compbiomed.2022.105623",
    "publishedDate": "2022-07-01T00:00:00+00:00",
    "source": "eupmc",
    "affiliations": [],
    "funders": [],
    "subjects": []
}
```

Example PhysioNet citation:
```json
  {
    "id": "0191cccf-1091-4095-ae4d-71f592ecd7a4",
    "created": "2025-07-24T13:16:09.783+00:00",
    "updated": "2025-08-13T17:39:06.682259+00:00",
    "repository": {
      "title": "PhysioNet",
      "external_id": null
    },
    "publisher": {
      "title": "Frontiers Media SA",
      "external_id": null
    },
    "journal": {
      "title": "Frontiers in Cardiovascular Medicine",
      "external_id": null
    },
    "title": "MIMIC-IV",
    "dataset": "https://doi.org/10.13026/s6n6-xd98",
    "publication": "https://doi.org/10.3389/fcvm.2022.879812",
    "publishedDate": "2022-05-27T00:00:00+00:00",
    "source": "eupmc",
    "affiliations": [],
    "funders": [],
    "subjects": []
}
```


## From OpenAlex

OpenAlex indexes papers, their citations, and some datasets. This looks like a useful resource to identify paper categories/topics, extract full text from papers, and find citations. However, OpenAlex does not index all datasets or citations. The database is also not fully up to date and is [300 GB large](https://docs.openalex.org/download-all-data/download-to-your-machine).


### Paper categories/topics and full text

The OpenAlex API has a limit of 100,000 calls per day, max 10/second (free!), and the snapshot is updated monthly for free download. They even provide a [guide for LLM usage](https://docs.openalex.org/api-guide-for-llms).

The API request to get the OpenAlex record for the DANDI example paper [https://doi.org/10.1098/rsif.2023.0288](https://doi.org/10.1098/rsif.2023.0288) is at [https://api.openalex.org/works/https://doi.org/10.1098/rsif.2023.0288](https://api.openalex.org/works/https://doi.org/10.1098/rsif.2023.0288). To get the journal name, look at "primary_location" -> "source". Topics, keywords, and concepts are under the "topics", "keywords", and "concepts" fields. The `"has_fulltext": false` field indicates that no full text is available, but the "open_access" -> "oa_url" field has a link to the publisher page for the paper. The paper also has a "location" with `"is_oa": true`, which has a source corresponding to PubMed Central and a "landing_page_url" of [https://www.ncbi.nlm.nih.gov/pmc/articles/10509587](https://www.ncbi.nlm.nih.gov/pmc/articles/10509587) which has the full text and PDF link.

It has concepts (categories/topics) such as "Computational biology" and "Fluid dynamics", and links to full text at [https://openalex.org/W2748058227/fulltext](https://openalex.org/W2748058227/fulltext) which includes a PDF link.

After looking at a few more papers, it seems that OpenAlex does not store the full text for the paper itself but provides links to external sources that have the full text, such as PubMed Central or publisher pages. However, it is able to do queries on the full text for search purposes, so I wonder if these full texts are stored somewhere in their backend and unavailable for public use. Using the search interface to search for "dandi neuroscience" in the full text returns some papers that mention DANDI. For example, [this paper](https://doi.org/10.1038/s41592-024-02232-7) ([OpenAlex API response](https://api.openalex.org/w4394577481)) mentions DANDI in the full text, and OpenAlex has a link to the full text at [https://openalex.org/W2932054860/fulltext](https://openalex.org/W2932054860/fulltext) which links to the publisher page.

It looks like the `open_access.any_repository_has_fulltext: true` value indicates that OpenAlex has indexed the full text for search purposes ( using [n-grams from sources like the Internet Archive](https://blog.openalex.org/fulltext-search-in-openalex/)), not necessarily that the full text is freely available. To find freely available full text, look at the `open_access.is_oa: true` filter and the `open_access.oa_url` field. Learn more about getting n-grams from the [OpenAlex API docs](https://docs.openalex.org/api-entities/works/get-n-grams), but note that as of December 6, 2025, the page says "The n-gram API endpoint is not currently in service."

We will likely require a different resource to get full texts of papers for improper dataset citation, such as PubMed Central Open Access Subset, bioRxiv/arXiv, or regularly pinging publisher APIs and PubMed Central for open access papers.

The topics, keywords, and concepts from OpenAlex can be useful for categorizing papers that cite datasets.


### Dataset citations

In the DANDI example above, [the paper](https://doi.org/10.1098/rsif.2023.0288) cites the DANDI dataset [https://doi.org/10.48324/dandi.000547/0.230822.1616](https://doi.org/10.48324/dandi.000547/0.230822.1616), and OpenAlex unfortunately does not index that citation or dataset. The paper cites 51 works in its bibliography. OpenAlex says that this paper cites 52 works, but lists only 49 of them, missing the DANDI dataset.

Similarly, in another DANDI example, [this paper](https://doi.org/10.1038/s41597-023-02214-y) cites the DANDI dataset [https://doi.org/10.48324/dandi.000037/0.230426.0054](https://doi.org/10.48324/dandi.000037/0.230426.0054), but OpenAlex does not index that citation or dataset.

Some newer papers (in 2025) are not indexed by OpenAlex at all, such as [this one](https://doi.org/10.1101/2024.04.28.591397) which cites a DANDI dataset. Its preprint (2024) is indexed though.

