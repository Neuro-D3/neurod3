"""
Citing-paper full text for the paper-mapping DAGs' citation step, fetched
concurrently.

Before this module, each DAG's citation loop fetched one citing paper's full
text at a time, inline, through ``_ensure_citing_paper_record``: about 7 s per
paper on average and 13.6 s for a paper with no open text (it tries every source
before giving up). A primary with 2,000 citing papers held one task for hours
while the network sat idle.

``ensure_citing_papers`` takes all citing papers of one primary paper, writes
the ones whose text is already cached straight away, and fetches the rest on a
thread pool. Each worker only does network and file work: it fetches the text
and writes the same JSON cache file the DAGs always wrote. Every database write
stays on the calling thread, one paper per commit as before, because a psycopg
connection is not thread-safe and because committing each paper upsert is what
keeps parallel mapped batches from deadlocking on shared DOIs.

Per-host request pacing and the headless-Chrome limit live in
``utils.paper_fulltext`` (``D3PaperFetcher``), so they hold across the threads.

    with citing_text_pool(params) as pool:
        for primary in ...:
            for doi, m in ensure_citing_papers(conn, cursor, papers, params=params,
                                               output_root=output_root, pool=pool):
                ...  # write this paper's citation edges

``fulltext_workers = 1`` gives the old serial behaviour, for comparisons.
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import requests

from utils.cache_keys import paper_cache_key_for_doi
from utils.find_reuse_core import Telemetry, normalize_doi
from utils.paper_fulltext import fetch_fulltext_oa
from utils.titles import clean_title

logger = logging.getLogger(__name__)

DEFAULT_FULLTEXT_WORKERS = 8
MAX_FULLTEXT_WORKERS = 32

PAPER_UPSERT_SQL = """
INSERT INTO papers (
    paper_doi, openalex_id, title, authors, publication_date, publication_year,
    fulltext_cache_key, fulltext_cached_at, fulltext_source, fulltext_available, fulltext_reason,
    source, journal, senior_author_country, fetched_at
)
VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (paper_doi) DO UPDATE SET
    openalex_id = COALESCE(EXCLUDED.openalex_id, papers.openalex_id),
    title = COALESCE(EXCLUDED.title, papers.title),
    authors = COALESCE(EXCLUDED.authors, papers.authors),
    publication_date = COALESCE(EXCLUDED.publication_date, papers.publication_date),
    publication_year = COALESCE(EXCLUDED.publication_year, papers.publication_year),
    fulltext_cache_key = COALESCE(EXCLUDED.fulltext_cache_key, papers.fulltext_cache_key),
    fulltext_cached_at = COALESCE(EXCLUDED.fulltext_cached_at, papers.fulltext_cached_at),
    fulltext_source = COALESCE(EXCLUDED.fulltext_source, papers.fulltext_source),
    fulltext_available = COALESCE(EXCLUDED.fulltext_available, papers.fulltext_available),
    fulltext_reason = COALESCE(EXCLUDED.fulltext_reason, papers.fulltext_reason),
    source = COALESCE(EXCLUDED.source, papers.source),
    journal = COALESCE(EXCLUDED.journal, papers.journal),
    senior_author_country = COALESCE(EXCLUDED.senior_author_country, papers.senior_author_country),
    fetched_at = NOW();
"""


def fulltext_workers(params: Mapping[str, Any]) -> int:
    """The ``fulltext_workers`` run parameter, clamped to 1..MAX_FULLTEXT_WORKERS."""
    try:
        n = int(params.get("fulltext_workers", DEFAULT_FULLTEXT_WORKERS))
    except (TypeError, ValueError):
        n = DEFAULT_FULLTEXT_WORKERS
    return max(1, min(n, MAX_FULLTEXT_WORKERS))


@contextmanager
def citing_text_pool(params: Mapping[str, Any]) -> Iterator[ThreadPoolExecutor]:
    """
    One thread pool for a whole citation batch.

    On the way out, queued fetches that have not started are cancelled, so an
    error on the calling thread does not wait for hundreds of downloads first.
    """
    pool = ThreadPoolExecutor(max_workers=fulltext_workers(params), thread_name_prefix="fulltext")
    try:
        yield pool
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def _citing_paper(citing: Mapping[str, Any], doi: str) -> Dict[str, Any]:
    """
    The papers-row fields the DAGs store for a citing paper from OpenAlex.

    Same fields as before this module: journal and senior-author country go
    on the citation edge, not on the citing paper's row.
    """
    return {
        "doi": doi,
        "openalex_id": citing.get("openalex_id"),
        "title": citing.get("title"),
        "authors": citing.get("authors"),
        "publication_date": citing.get("publication_date"),
        "publication_year": citing.get("publication_year"),
        "source": "openalex_citation",
    }


def fetch_and_cache_text(doi: str, paper: Mapping[str, Any], *, output_root: Path,
                         params: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Fetch one paper's full text and write the DAGs' JSON cache file for it.

    Runs on a worker thread: no database access. Never raises, so one bad
    paper cannot take down the batch. A fetch or cache write that errors is
    not recorded (no cache key is returned), so the next run tries it again.
    """
    cache_key = paper_cache_key_for_doi(doi)
    if not cache_key:
        return {"cache_key": None, "cached_at": None, "source": "none", "available": False,
                "reason": "invalid_doi", "fetched": False}
    try:
        return _fetch_and_write(doi, paper, cache_key, output_root=output_root, params=params)
    except Exception as exc:
        logger.warning("Full-text fetch or cache write failed doi=%s err=%s", doi, exc)
        return {"cache_key": None, "cached_at": None, "source": None, "available": None,
                "reason": None, "fetched": False}


def _fetch_and_write(doi: str, paper: Mapping[str, Any], cache_key: str, *, output_root: Path,
                     params: Mapping[str, Any]) -> Dict[str, Any]:
    # Only the legacy Europe PMC / NCBI path uses this session; paper-text-fetcher
    # keeps its own per thread.
    with requests.Session() as session:
        full_text, src, available, reason = fetch_fulltext_oa(
            session,
            doi,
            telemetry=Telemetry(),
            min_interval_seconds=float(params.get("min_api_interval_seconds", 0.2)),
            max_retries=int(params.get("max_retries", 6)),
            backoff_seconds=float(params.get("backoff_seconds", 2.0)),
        )

    cache_path = Path(output_root) / cache_key
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "doi": doi,
                "title": paper.get("title"),
                "authors": paper.get("authors"),
                "canonical_url": f"https://doi.org/{doi}",
                "openalex_id": paper.get("openalex_id"),
                "publication_date": paper.get("publication_date"),
                "publication_year": paper.get("publication_year"),
                "full_text": full_text,
                "text": full_text,
                "full_text_source": src,
                "full_text_available": bool(available),
                "full_text_reason": reason,
                "cached_at": datetime.now(timezone.utc).isoformat(),
            },
            f,
            ensure_ascii=False,
        )
    return {"cache_key": cache_key, "cached_at": datetime.now(timezone.utc), "source": src,
            "available": bool(available), "reason": reason, "fetched": True}


def upsert_citing_paper(cursor: Any, paper: Mapping[str, Any], text: Optional[Mapping[str, Any]]) -> None:
    """Write one citing paper's papers row. ``text`` is None for a paper whose text is already cached."""
    text = text or {}
    authors = paper.get("authors")
    cursor.execute(
        PAPER_UPSERT_SQL,
        (
            paper["doi"],
            paper.get("openalex_id"),
            clean_title(paper.get("title")),
            json.dumps(authors) if authors is not None else None,
            paper.get("publication_date"),
            paper.get("publication_year"),
            text.get("cache_key"),
            text.get("cached_at"),
            text.get("source"),
            text.get("available"),
            text.get("reason"),
            paper.get("source"),
            paper.get("journal"),
            paper.get("senior_author_country"),
        ),
    )


def _existing_cache_keys(cursor: Any, dois: Sequence[str]) -> Dict[str, Optional[str]]:
    if not dois:
        return {}
    cursor.execute(
        "SELECT paper_doi, fulltext_cache_key FROM papers WHERE paper_doi = ANY(%s)",
        (list(dois),),
    )
    return {row[0]: row[1] for row in cursor.fetchall()}


def _metrics(*, already_cached: int = 0, fetched: int = 0, unavailable: int = 0) -> Dict[str, int]:
    return {"paper_upserted": 1, "already_cached": already_cached,
            "fulltext_fetched": fetched, "fulltext_unavailable": unavailable}


def ensure_citing_papers(
    conn: Any,
    cursor: Any,
    citing_papers: Sequence[Mapping[str, Any]],
    *,
    params: Mapping[str, Any],
    output_root: Path,
    pool: ThreadPoolExecutor,
) -> Iterator[Tuple[str, Dict[str, int]]]:
    """
    Make sure every citing paper has a papers row and, where possible, cached text.

    Yields ``(doi, metrics)`` once that paper's row is committed, so the caller
    can write its citation edges. Papers whose text is already cached come
    first; fetched ones follow in the order their downloads finish. Citing
    papers without a valid DOI are skipped; a DOI listed twice is handled once.
    ``metrics`` has the keys the DAGs count: paper_upserted, already_cached,
    fulltext_fetched, fulltext_unavailable.
    """
    papers: Dict[str, Dict[str, Any]] = {}
    for citing in citing_papers:
        doi = normalize_doi(citing.get("doi"))
        if doi and doi not in papers:
            papers[doi] = _citing_paper(citing, doi)
    if not papers:
        return

    force = bool(params.get("force_refresh_fulltext", False))
    existing = _existing_cache_keys(cursor, list(papers))
    to_fetch: List[str] = []
    for doi, paper in papers.items():
        if existing.get(doi) and not force:
            upsert_citing_paper(cursor, paper, None)
            conn.commit()
            yield doi, _metrics(already_cached=1)
        else:
            to_fetch.append(doi)

    futures: Dict[Future, str] = {
        pool.submit(fetch_and_cache_text, doi, papers[doi], output_root=output_root, params=params): doi
        for doi in to_fetch
    }
    try:
        for future in as_completed(futures):
            doi = futures[future]
            text = future.result()
            upsert_citing_paper(cursor, papers[doi], text)
            conn.commit()
            if text.get("available"):
                yield doi, _metrics(fetched=1)
            else:
                yield doi, _metrics(unavailable=1)
    finally:
        # The caller stopped early or failed: drop this primary's downloads that never started.
        for future in futures:
            future.cancel()
