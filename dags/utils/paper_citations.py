"""
Shared citation helpers adapted from the find_reuse project.

Scope:
- alternate preprint/published DOI linking
- OpenAlex citing-paper lookup
- citation-context extraction against cached paper text

This module is intentionally deterministic and token-free so it can run inside
the archive-specific mapping DAGs.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from utils.find_reuse_core import (
    Telemetry,
    http_get_json,
    normalize_doi,
    publication_year_from_date,
)

logger = logging.getLogger(__name__)

_PAPER_METADATA_TIMEOUT = 30


def normalize_openalex_id(openalex_id: Optional[str]) -> Optional[str]:
    if not isinstance(openalex_id, str) or not openalex_id.strip():
        return None
    value = openalex_id.strip()
    if "/" in value:
        value = value.rsplit("/", 1)[-1]
    return value or None


def get_openalex_paper_data(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> Dict[str, Any]:
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return {
            "publication_date": None,
            "publication_year": None,
            "cited_by_count": None,
            "openalex_id": None,
            "title": None,
            "authors": None,
        }

    url = f"https://api.openalex.org/works/doi:{quote(doi_norm)}"
    data = http_get_json(
        session,
        url,
        timeout=20,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not data:
        return {
            "publication_date": None,
            "publication_year": None,
            "cited_by_count": None,
            "openalex_id": None,
            "title": None,
            "authors": None,
        }

    authorships = data.get("authorships")
    authors_out: Optional[List[str]] = None
    if isinstance(authorships, list):
        names: List[str] = []
        for authorship in authorships:
            if not isinstance(authorship, dict):
                continue
            author = authorship.get("author")
            if isinstance(author, dict):
                display_name = author.get("display_name")
                if isinstance(display_name, str) and display_name.strip():
                    names.append(display_name.strip())
        if names:
            deduped: List[str] = []
            for name in names:
                if name not in deduped:
                    deduped.append(name)
            authors_out = deduped or None

    publication_date = data.get("publication_date")
    publication_date_out = publication_date.strip() if isinstance(publication_date, str) and publication_date.strip() else None
    return {
        "publication_date": publication_date_out,
        "publication_year": publication_year_from_date(publication_date_out),
        "cited_by_count": data.get("cited_by_count"),
        "openalex_id": data.get("id") if isinstance(data.get("id"), str) else None,
        "title": data.get("title") if isinstance(data.get("title"), str) else None,
        "authors": authors_out,
    }


def _lookup_published_version(
    session: requests.Session,
    preprint_doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> Optional[str]:
    doi_norm = normalize_doi(preprint_doi)
    if not doi_norm or not doi_norm.lower().startswith("10.1101/"):
        return None

    for server in ("biorxiv", "medrxiv"):
        url = f"https://api.biorxiv.org/pubs/{server}/{quote(doi_norm)}"
        data = http_get_json(
            session,
            url,
            timeout=_PAPER_METADATA_TIMEOUT,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
            telemetry=telemetry,
        )
        if not isinstance(data, dict):
            continue
        collection = data.get("collection")
        if not isinstance(collection, list) or not collection:
            continue
        item = collection[0]
        if not isinstance(item, dict):
            continue
        published_doi = normalize_doi(item.get("published_doi"))
        if published_doi:
            return published_doi
    return None


def _sanitize_openalex_title_search_term(title: str) -> str:
    """
    Remove characters that break OpenAlex `title.search:"..."` filter syntax
    (notably embedded double quotes and commas that delimit filter clauses).
    """
    t = title.strip().replace('"', " ").replace(",", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _lookup_preprint_version(
    session: requests.Session,
    published_doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> Optional[str]:
    published_norm = normalize_doi(published_doi)
    if not published_norm or published_norm.lower().startswith("10.1101/"):
        return None

    paper = get_openalex_paper_data(
        session,
        published_norm,
        telemetry=telemetry,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )
    title = paper.get("title")
    if not isinstance(title, str) or len(title.strip()) < 10:
        return None

    safe_title = _sanitize_openalex_title_search_term(title)
    if len(safe_title) < 10:
        return None

    filter_value = f'title.search:"{safe_title}",type:preprint'
    url = (
        "https://api.openalex.org/works"
        f"?filter={quote(filter_value)}&select=doi,title,id,publication_date&per_page=5"
    )
    data = http_get_json(
        session,
        url,
        timeout=20,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not isinstance(data, dict):
        return None
    results = data.get("results")
    if not isinstance(results, list):
        return None

    title_norm = title.strip().lower()
    for work in results:
        if not isinstance(work, dict):
            continue
        work_doi = normalize_doi(str(work.get("doi", "")).replace("https://doi.org/", ""))
        work_title = work.get("title")
        if (
            work_doi
            and work_doi.lower().startswith("10.1101/")
            and isinstance(work_title, str)
            and work_title.strip().lower() == title_norm
        ):
            return work_doi
    return None


def get_alternate_doi(
    session: requests.Session,
    doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> Optional[str]:
    doi_norm = normalize_doi(doi)
    if not doi_norm:
        return None
    if doi_norm.lower().startswith("10.1101/"):
        return _lookup_published_version(
            session,
            doi_norm,
            telemetry=telemetry,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
    return _lookup_preprint_version(
        session,
        doi_norm,
        telemetry=telemetry,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )


def get_citing_papers(
    session: requests.Session,
    openalex_id: str,
    after_date: str,
    *,
    telemetry: Telemetry,
    max_results: int = 10,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> List[Dict[str, Any]]:
    openalex_norm = normalize_openalex_id(openalex_id)
    if not openalex_norm or not after_date:
        return []

    per_page = min(max(int(max_results or 0), 1), 200)
    cursor = "*"
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    while cursor and len(out) < max_results:
        filter_value = f"cites:{openalex_norm},publication_date:>{after_date}"
        url = (
            "https://api.openalex.org/works"
            f"?filter={quote(filter_value)}&per_page={per_page}&cursor={quote(cursor)}"
        )
        data = http_get_json(
            session,
            url,
            timeout=30,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
            telemetry=telemetry,
        )
        if not isinstance(data, dict):
            break
        results = data.get("results")
        if not isinstance(results, list) or not results:
            break

        for work in results:
            if not isinstance(work, dict):
                continue
            doi = normalize_doi(str(work.get("doi", "")).replace("https://doi.org/", ""))
            openalex_work_id = work.get("id") if isinstance(work.get("id"), str) else None
            dedupe_key = doi or normalize_openalex_id(openalex_work_id)
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            authors_out: Optional[List[str]] = None
            authorships = work.get("authorships")
            if isinstance(authorships, list):
                names: List[str] = []
                for authorship in authorships:
                    if not isinstance(authorship, dict):
                        continue
                    author = authorship.get("author")
                    if isinstance(author, dict):
                        display_name = author.get("display_name")
                        if isinstance(display_name, str) and display_name.strip():
                            names.append(display_name.strip())
                if names:
                    deduped: List[str] = []
                    for name in names:
                        if name not in deduped:
                            deduped.append(name)
                    authors_out = deduped or None

            publication_date = work.get("publication_date")
            publication_date_out = publication_date.strip() if isinstance(publication_date, str) and publication_date.strip() else None
            out.append(
                {
                    "doi": doi,
                    "openalex_id": openalex_work_id,
                    "title": work.get("title") if isinstance(work.get("title"), str) else None,
                    "publication_date": publication_date_out,
                    "publication_year": publication_year_from_date(publication_date_out),
                    "authors": authors_out,
                    "citation_source": "openalex",
                }
            )
            if len(out) >= max_results:
                break

        meta = data.get("meta")
        cursor = meta.get("next_cursor") if isinstance(meta, dict) else None
        if not isinstance(cursor, str) or not cursor.strip():
            break

    return out


def get_cited_paper_metadata(
    session: requests.Session,
    cited_doi: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
    seeded_title: Optional[str] = None,
    seeded_authors: Optional[List[str]] = None,
    seeded_year: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    doi_norm = normalize_doi(cited_doi)
    if not doi_norm:
        return None

    if seeded_title and seeded_authors and seeded_year:
        return {
            "authors": _author_last_names(seeded_authors),
            "year": seeded_year,
            "title": seeded_title,
            "doi": doi_norm,
        }

    url = f"https://api.crossref.org/works/{quote(doi_norm)}"
    data = http_get_json(
        session,
        url,
        timeout=_PAPER_METADATA_TIMEOUT,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    if not isinstance(data, dict):
        if seeded_title or seeded_authors or seeded_year:
            return {
                "authors": _author_last_names(seeded_authors or []),
                "year": seeded_year,
                "title": seeded_title,
                "doi": doi_norm,
            }
        return None

    message = data.get("message")
    if not isinstance(message, dict):
        return None

    authors: List[str] = []
    for author in message.get("author", []) if isinstance(message.get("author"), list) else []:
        if not isinstance(author, dict):
            continue
        family = author.get("family")
        if isinstance(family, str) and family.strip():
            authors.append(family.strip())

    year: Optional[int] = None
    for date_field in ("published-print", "published-online", "published", "created"):
        date_info = message.get(date_field)
        if not isinstance(date_info, dict):
            continue
        date_parts = date_info.get("date-parts")
        if isinstance(date_parts, list) and date_parts and isinstance(date_parts[0], list) and date_parts[0]:
            try:
                year = int(date_parts[0][0])
                break
            except Exception:
                continue

    title = None
    raw_title = message.get("title")
    if isinstance(raw_title, list) and raw_title and isinstance(raw_title[0], str) and raw_title[0].strip():
        title = raw_title[0].strip()
    elif isinstance(raw_title, str) and raw_title.strip():
        title = raw_title.strip()

    return {
        "authors": authors or _author_last_names(seeded_authors or []),
        "year": year or seeded_year,
        "title": title or seeded_title,
        "doi": doi_norm,
    }


def _author_last_names(authors: List[str]) -> List[str]:
    out: List[str] = []
    for author in authors:
        if not isinstance(author, str):
            continue
        parts = [part for part in author.strip().split() if part]
        if not parts:
            continue
        out.append(parts[-1])
    return out


def find_reference_section_start(text: str) -> int:
    doi_positions = [m.start() for m in re.finditer(r"10\.\d{4}/", text)]
    if len(doi_positions) < 4:
        return len(text)
    for i in range(len(doi_positions) - 3):
        span = doi_positions[i + 3] - doi_positions[i]
        if span < 1000:
            if i + 10 < len(doi_positions):
                next_span = doi_positions[i + 10] - doi_positions[i]
                if next_span < 5000:
                    return doi_positions[i]
            else:
                return doi_positions[i]
    return len(text)


def find_reference_number_for_doi(text: str, doi: str) -> Optional[int]:
    doi_escaped = re.escape(doi)
    doi_match = re.search(doi_escaped, text, re.IGNORECASE)
    if not doi_match:
        return None

    doi_pos = doi_match.start()
    search_start = max(0, doi_pos - 500)
    preceding_text = text[search_start:doi_pos]

    ref_pattern = r"(?:^|\n)\s*(\d{1,3})(?:\.(?!\d)|[\s\)])(?![\d/])"
    ref_numbers = list(re.finditer(ref_pattern, preceding_text))
    valid_refs: List[int] = []
    for match in ref_numbers:
        num = int(match.group(1))
        match_end = match.end()
        remaining = preceding_text[match_end:match_end + 20] if match_end < len(preceding_text) else ""
        if num == 10 and re.match(r"\d{4}/", remaining):
            continue
        valid_refs.append(num)
    if valid_refs:
        return valid_refs[-1]

    ref_pattern_b = r"\s(\d{1,3})\.\s+[A-Z]"
    ref_numbers_b = list(re.finditer(ref_pattern_b, preceding_text))
    valid_refs_b: List[int] = []
    for match in ref_numbers_b:
        num = int(match.group(1))
        if num == 10:
            remaining = preceding_text[match.end() - 1:match.end() + 20]
            if re.match(r"\d{4}/", remaining):
                continue
        valid_refs_b.append(num)
    if valid_refs_b:
        return valid_refs_b[-1]

    ref_start = find_reference_section_start(text)
    if ref_start < len(text):
        ref_section = text[ref_start:]
        ref_dois = list(re.finditer(r"10\.\d{4,}/[^\s]+", ref_section))
        seen_dois: set[str] = set()
        ref_number_counter = 0
        for match in ref_dois:
            doi_text = match.group().lower().rstrip(".,;:)")
            if doi_text not in seen_dois:
                seen_dois.add(doi_text)
                ref_number_counter += 1
                if doi.lower() in doi_text:
                    return ref_number_counter
    return None


def find_numbered_citations(text: str, ref_number: int) -> List[int]:
    positions: List[int] = []
    ref_str = str(ref_number)

    for match in re.finditer(r"\[([^\]]*)\]", text):
        content = match.group(1)
        if re.search(rf"\b{ref_str}\b", content):
            positions.append(match.start())
        elif "-" in content or "–" in content:
            for range_match in re.finditer(r"(\d+)\s*[-–]\s*(\d+)", content):
                start_num = int(range_match.group(1))
                end_num = int(range_match.group(2))
                if start_num <= ref_number <= end_num:
                    positions.append(match.start())
                    break

    paren_cite_pattern = r"\((\d{1,3}(?:\s*[,–-]\s*\d{1,3})*)\)"
    for match in re.finditer(paren_cite_pattern, text):
        content = match.group(1)
        if re.search(r"\b\d{4}\b", content):
            continue
        if re.search(rf"\b{ref_str}\b", content):
            positions.append(match.start())
        elif "-" in content or "–" in content:
            for range_match in re.finditer(r"(\d+)\s*[-–]\s*(\d+)", content):
                start_num = int(range_match.group(1))
                end_num = int(range_match.group(2))
                if start_num <= ref_number <= end_num:
                    positions.append(match.start())
                    break

    for match in re.finditer(rf"[a-zA-Z]({ref_str})(?:[,\s]|$)", text):
        positions.append(match.start())

    for match in re.finditer(r"[a-zA-Z](\d{1,3}(?:,\d{1,3})+)", text):
        numbers = [int(n) for n in match.group(1).split(",")]
        if ref_number in numbers:
            positions.append(match.start())

    for match in re.finditer(rf"[a-zA-Z]\s+{ref_str}(?:\s*[,–-]\s*\d+)*(?:\s|$|[,.])", text):
        context = text[max(0, match.start() - 5):min(len(text), match.end() + 20)]
        if len(re.findall(r"\b\d{1,3}\b", context)) >= 1:
            positions.append(match.start())

    for match in re.finditer(rf"\)\s*{ref_str}(?:\s*[,–-]\s*\d+)*(?:\s|$|[,.])", text):
        positions.append(match.start())

    range_pattern = r"(\d{1,3})\s*[–-]\s*(\d{1,3})"
    for match in re.finditer(range_pattern, text):
        start_num = int(match.group(1))
        end_num = int(match.group(2))
        if not (start_num <= ref_number <= end_num):
            continue
        before = text[max(0, match.start() - 30):match.start()]
        after = text[match.end():min(len(text), match.end() + 30)]
        skip_patterns = [
            r"10\.",
            r"0000-",
            r"\d{4}-\d{4}",
            r"[kMG]?Hz",
            r"[kMG]?Ω",
            r"\d+\s*[kMG]?[Ωω]",
            r"[A-Z]\d+x\d+",
            r"-\d{2,4}\.",
        ]
        should_skip = False
        for pattern in skip_patterns:
            if re.search(pattern, before + match.group() + after, re.IGNORECASE):
                should_skip = True
                break
        if re.match(r"\s*[kMG]?[HzΩωms%°]", after, re.IGNORECASE):
            should_skip = True
        if re.search(r"\d\s*$", before):
            should_skip = True
        if not should_skip and re.search(r"[a-zA-Z]\s*$", before):
            positions.append(match.start())

    return list(set(positions))


def normalize_author_name(name: str) -> str:
    import unicodedata

    normalized = unicodedata.normalize("NFKD", name)
    return "".join(c for c in normalized if not unicodedata.combining(c))


def find_author_citations(text: str, authors: List[str], year: int, year_tolerance: int = 1) -> List[int]:
    if not authors or not year:
        return []

    positions: List[int] = []
    first_author = authors[0]
    first_author_normalized = normalize_author_name(first_author)
    years_to_search = [str(year)]
    for delta in range(1, year_tolerance + 1):
        years_to_search.append(str(year - delta))
        years_to_search.append(str(year + delta))

    author_patterns = [re.escape(first_author)]
    if first_author_normalized != first_author:
        author_patterns.append(re.escape(first_author_normalized))

    patterns: List[str] = []
    for author_esc in author_patterns:
        for year_str in years_to_search:
            if len(authors) == 1:
                patterns.extend(
                    [
                        rf"\({author_esc}\s*,?\s*{year_str}[a-z]?\)",
                        rf"{author_esc}\s*\({year_str}[a-z]?\)",
                        rf"{author_esc}\s*,\s*{year_str}[a-z]?",
                    ]
                )
            elif len(authors) == 2:
                second_author = authors[1]
                second_patterns = {re.escape(second_author), re.escape(normalize_author_name(second_author))}
                for second_esc in second_patterns:
                    patterns.extend(
                        [
                            rf"\({author_esc}\s+(?:and|&)\s+{second_esc}\s*,?\s*{year_str}[a-z]?\)",
                            rf"{author_esc}\s+(?:and|&)\s+{second_esc}\s*\({year_str}[a-z]?\)",
                            rf"{author_esc}\s+(?:and|&)\s+{second_esc}\s*,\s*{year_str}[a-z]?",
                        ]
                    )

            if len(authors) >= 2:
                patterns.extend(
                    [
                        rf"\({author_esc}\s+et\s+al\.?\s*,?\s*{year_str}[a-z]?\)",
                        rf"{author_esc}\s+et\s+al\.?\s*\({year_str}[a-z]?\)",
                        rf"{author_esc}\s+et\s+al\.?\s*,\s*{year_str}[a-z]?",
                        rf"{author_esc}\s+et\s+al\.?\s+{year_str}[a-z]?",
                    ]
                )

        if len(authors) == 1:
            patterns.append(rf"{author_esc}\s*\(\d+\)")
        elif len(authors) == 2:
            second_author = authors[1]
            second_patterns = {re.escape(second_author), re.escape(normalize_author_name(second_author))}
            for second_esc in second_patterns:
                patterns.append(rf"{author_esc}\s+(?:and|&)\s+{second_esc}\s*\(\d+\)")
        if len(authors) >= 2:
            patterns.append(rf"{author_esc}\s+et\s+al\.?\s*\(\d+\)")

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            positions.append(match.start())
    return positions


def find_title_mentions(text: str, title: str) -> List[int]:
    if not title or len(title) < 20:
        return []

    positions: List[int] = []
    stop_words = {"the", "a", "an", "of", "in", "on", "for", "and", "or", "to", "with"}
    words = [word for word in title.split() if word.lower() not in stop_words]
    if len(words) >= 3:
        search_phrase = " ".join(words[: min(5, len(words))])
        for match in re.finditer(re.escape(search_phrase), text, re.IGNORECASE):
            positions.append(match.start())
    return positions


def extract_context(text: str, position: int, context_chars: int = 500) -> Dict[str, Any]:
    start = max(0, position - context_chars)
    end = min(len(text), position + context_chars)

    if start > 0:
        search_region = text[max(0, start - 100):start]
        sent_end = max(
            search_region.rfind(". "),
            search_region.rfind(".\n"),
            search_region.rfind("? "),
            search_region.rfind("! "),
        )
        if sent_end != -1:
            start = max(0, start - 100) + sent_end + 2

    if end < len(text):
        search_region = text[end:min(len(text), end + 100)]
        sent_end = min(
            search_region.find(". ") if search_region.find(". ") != -1 else 9999,
            search_region.find(".\n") if search_region.find(".\n") != -1 else 9999,
            search_region.find("? ") if search_region.find("? ") != -1 else 9999,
            search_region.find("! ") if search_region.find("! ") != -1 else 9999,
        )
        if sent_end != 9999:
            end = end + sent_end + 1

    return {
        "context": text[start:end].strip(),
        "start": start,
        "end": end,
        "citation_position": position,
    }


def is_in_reference_section(text: str, position: int) -> bool:
    text_before = text[max(0, position - 5000):position].lower()
    ref_markers = ["references\n", "bibliography\n", "literature cited", "works cited"]
    for marker in ref_markers:
        if marker in text_before:
            marker_pos = text_before.rfind(marker)
            text_after_marker = text_before[marker_pos:]
            if len(re.findall(r"10\.\d{4}/", text_after_marker)) > 3:
                return True

    surrounding = text[max(0, position - 200):min(len(text), position + 200)]
    return len(re.findall(r"10\.\d{4}/", surrounding)) > 2


def find_citation_contexts(
    citing_paper_text: str,
    cited_doi: str,
    *,
    cited_title: Optional[str] = None,
    cited_authors: Optional[List[str]] = None,
    cited_year: Optional[int] = None,
    context_chars: int = 500,
    session: Optional[requests.Session] = None,
    telemetry: Optional[Telemetry] = None,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
    exclude_reference_section: bool = True,
) -> List[Dict[str, Any]]:
    working_session = session or requests.Session()
    working_telemetry = telemetry or Telemetry()
    metadata = get_cited_paper_metadata(
        working_session,
        cited_doi,
        telemetry=working_telemetry,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        seeded_title=cited_title,
        seeded_authors=cited_authors,
        seeded_year=cited_year,
    )
    if not metadata:
        return []

    results: List[Dict[str, Any]] = []
    seen_positions: set[int] = set()
    ref_number = find_reference_number_for_doi(citing_paper_text, metadata["doi"])
    if ref_number:
        positions = find_numbered_citations(citing_paper_text, ref_number)
        for pos in positions:
            pos_bucket = pos // 100
            if pos_bucket in seen_positions:
                continue
            if exclude_reference_section and is_in_reference_section(citing_paper_text, pos):
                continue
            seen_positions.add(pos_bucket)
            ctx = extract_context(citing_paper_text, pos, context_chars)
            ctx["method"] = "numbered_citation"
            ctx["reference_number"] = ref_number
            results.append(ctx)

    if metadata.get("authors") and metadata.get("year"):
        positions = find_author_citations(
            citing_paper_text,
            list(metadata["authors"]),
            int(metadata["year"]),
        )
        for pos in positions:
            pos_bucket = pos // 100
            if pos_bucket in seen_positions:
                continue
            if exclude_reference_section and is_in_reference_section(citing_paper_text, pos):
                continue
            seen_positions.add(pos_bucket)
            ctx = extract_context(citing_paper_text, pos, context_chars)
            ctx["method"] = "author_year"
            ctx["authors"] = metadata["authors"]
            ctx["year"] = metadata["year"]
            results.append(ctx)

    if metadata.get("title"):
        positions = find_title_mentions(citing_paper_text, str(metadata["title"]))
        for pos in positions:
            pos_bucket = pos // 100
            if pos_bucket in seen_positions:
                continue
            if exclude_reference_section and is_in_reference_section(citing_paper_text, pos):
                continue
            seen_positions.add(pos_bucket)
            ctx = extract_context(citing_paper_text, pos, context_chars)
            ctx["method"] = "title_mention"
            ctx["title"] = metadata["title"]
            results.append(ctx)

    results.sort(key=lambda item: item["citation_position"])
    for result in results:
        result["cited_doi"] = metadata["doi"]
        result["cited_metadata"] = metadata
    return results
