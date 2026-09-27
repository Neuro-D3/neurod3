"""
Find and remove primary papers whose DOI was cut off mid-way.

Paper mapping used to read DOIs from OpenNeuro's 256-character description,
which can end mid-DOI (ds003509 got `10.1016/j.neur`). Mapping now reads the
full text and drops such fragments (find_reuse_core.drop_cut_off_papers); this
removes the ones stored before that.

A mapped primary DOI counts as cut off when no registry knew it (no title, no
OpenAlex id) and a longer DOI starting with it is mapped to the same dataset or
written in the dataset's description.

    cd /opt/airflow/dags && python -m utils.cut_off_doi_cleanup          # list only
    cd /opt/airflow/dags && python -m utils.cut_off_doi_cleanup --apply  # delete
"""

import argparse
from typing import Dict, Iterable, List, Optional, Tuple

from utils.find_reuse_core import extract_dois_from_text

# (table prefix, dataset id column) per archive.
ARCHIVES: List[Tuple[str, str]] = [
    ("dandi", "dandi_id"),
    ("openneuro", "openneuro_id"),
    ("crcns", "crcns_id"),
    ("sparc", "sparc_id"),
]


def cut_off_dois(mapped: Dict[str, bool], other_dois: Iterable[str]) -> List[str]:
    """
    The mapped DOIs that are cut off. `mapped` is DOI -> resolved (has a title
    or an OpenAlex id); `other_dois` are DOIs found in the dataset's text.
    """
    known = {d.lower() for d in mapped} | {d.lower() for d in other_dois}
    return sorted(
        doi for doi, resolved in mapped.items()
        if not resolved and any(other != doi.lower() and other.startswith(doi.lower()) for other in known)
    )


def _existing(cursor, table: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s;",
        (table,),
    )
    return cursor.fetchone() is not None


def _columns(cursor, table: str) -> set:
    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s;",
        (table,),
    )
    return {r[0] for r in cursor.fetchall()}


def find_cut_off(cursor) -> List[Tuple[str, str, str, str]]:
    """(archive prefix, id column, dataset id, DOI) for every cut-off primary DOI."""
    found: List[Tuple[str, str, str, str]] = []
    for prefix, id_col in ARCHIVES:
        if not (_existing(cursor, f"{prefix}_paper_map") and _existing(cursor, f"{prefix}_dataset")):
            continue
        text_cols = [c for c in ("description", "full_description") if c in _columns(cursor, f"{prefix}_dataset")]
        text_sql = f"concat_ws(' ', {', '.join('d.' + c for c in text_cols)})" if text_cols else "''"
        cursor.execute(
            f"""
            SELECT m.{id_col}, m.paper_doi, (p.title IS NOT NULL OR p.openalex_id IS NOT NULL), {text_sql}
            FROM {prefix}_paper_map m
            LEFT JOIN papers p ON p.paper_doi = m.paper_doi
            LEFT JOIN {prefix}_dataset d ON d.dataset_id = m.{id_col};
            """
        )
        by_dataset: Dict[str, Tuple[Dict[str, bool], set]] = {}
        for dataset_id, doi, resolved, text in cursor.fetchall():
            mapped, text_dois = by_dataset.setdefault(str(dataset_id), ({}, set()))
            mapped[doi] = bool(resolved)
            text_dois.update(extract_dois_from_text(text or ""))
        for dataset_id, (mapped, text_dois) in sorted(by_dataset.items()):
            for doi in cut_off_dois(mapped, text_dois):
                found.append((prefix, id_col, dataset_id, doi))
    return found


def delete_cut_off(cursor, rows: List[Tuple[str, str, str, str]]) -> Dict[str, int]:
    """
    Remove each cut-off DOI from its dataset (labels, citation edges, mapping),
    then the paper itself once nothing else refers to it.
    """
    deleted = {"classifications": 0, "citations": 0, "mappings": 0, "papers": 0}
    for prefix, id_col, dataset_id, doi in rows:
        for table, key, column in (
            (f"{prefix}_paper_citation_classifications", "classifications", "primary_paper_doi"),
            (f"{prefix}_paper_citations", "citations", "primary_paper_doi"),
            (f"{prefix}_paper_map", "mappings", "paper_doi"),
        ):
            if _existing(cursor, table):
                cursor.execute(f"DELETE FROM {table} WHERE {id_col} = %s AND {column} = %s;", (dataset_id, doi))
                deleted[key] += cursor.rowcount
    references = []
    for prefix, _ in ARCHIVES:
        for table, columns in (
            (f"{prefix}_paper_map", ("paper_doi",)),
            (f"{prefix}_paper_citations", ("primary_paper_doi", "citing_paper_doi")),
            (f"{prefix}_paper_citation_classifications", ("primary_paper_doi", "citing_paper_doi")),
        ):
            if _existing(cursor, table):
                references += [f"SELECT 1 FROM {table} WHERE {c} = p.paper_doi" for c in columns]
    unreferenced = " AND ".join(f"NOT EXISTS ({r})" for r in references) or "TRUE"
    for doi in sorted({row[3] for row in rows}):
        cursor.execute(f"DELETE FROM papers p WHERE p.paper_doi = %s AND {unreferenced};", (doi,))
        deleted["papers"] += cursor.rowcount
    return deleted


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="delete what is found (default: list only)")
    args = parser.parse_args(argv)

    from utils.database import get_db_connection

    with get_db_connection() as conn:
        cursor = conn.cursor()
        rows = find_cut_off(cursor)
        for prefix, _, dataset_id, doi in rows:
            print(f"{prefix:<10} {dataset_id:<24} {doi}")
        print(f"{len(rows)} cut-off primary DOI(s)")
        if args.apply and rows:
            print("deleted:", delete_cut_off(cursor, rows))
            conn.commit()
        elif rows:
            print("list only; run with --apply to delete")


if __name__ == "__main__":
    main()
