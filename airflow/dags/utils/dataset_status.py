"""
Dataset status: where every ingested dataset stands in paper mapping.

Each archive's dataset table gets two columns:

``dataset_status``
    ``mapped``    at least one row in the archive's paper map
    ``no_paper``  mapping ran and the archive's metadata gave no paper (``papers = 0``)
    ``pending``   never tried, or a previous attempt failed for a transient reason
    ``excluded``  junk: a test / placeholder / empty dataset. Never mapped, and
                  left out of the site's dataset counts and list.
``dataset_status_reason``
    why a dataset is ``excluded`` (``placeholder_title``, ``title_keyword:test``,
    ``empty``, ``find_reuse_test_id``, ...); NULL for every other status.

``refresh_dataset_status`` recomputes both columns for a whole archive from the
dataset table and its paper map. It is cheap (a few thousand rows) and runs at the
end of every ingestion run and at the start and end of every paper-mapping run, so
the columns are always current when the API reads them.

Junk rules follow the find_reuse repo where it has one: DANDI drops empty
dandisets (no assets) and keeps a curated list of test dandisets; SPARC drops
datasets tagged test/embargo (done at ingestion, see sparc_ingestion); CRCNS is
curated and has none. On top of that the title (not the description) is checked
for placeholder names and test/dummy keywords. The old mapping-DAG filter also
matched "sample", "benchmark" and "synthetic" anywhere in the description and
skipped real datasets (the NLB and FALCON benchmark dandisets, "test-retest"
fMRI, "tissue sample" everywhere); those words are gone on purpose.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from utils.database import apply_schema_ddl

logger = logging.getLogger(__name__)

STATUS_MAPPED = "mapped"
STATUS_NO_PAPER = "no_paper"
STATUS_PENDING = "pending"
STATUS_EXCLUDED = "excluded"
ALL_STATUSES = (STATUS_MAPPED, STATUS_NO_PAPER, STATUS_PENDING, STATUS_EXCLUDED)

# source label -> (dataset table, paper map table, paper map's dataset id column)
SOURCE_TABLES: Dict[str, Tuple[str, str, str]] = {
    "DANDI": ("dandi_dataset", "dandi_paper_map", "dandi_id"),
    "OpenNeuro": ("openneuro_dataset", "openneuro_paper_map", "openneuro_id"),
    "CRCNS": ("crcns_dataset", "crcns_paper_map", "crcns_id"),
    "SPARC": ("sparc_dataset", "sparc_paper_map", "sparc_id"),
}

# Whole-word title keywords that mark a dataset as test/dummy. "test-retest" is a
# real study design and is stripped before matching.
TITLE_JUNK_KEYWORDS: Tuple[str, ...] = ("test", "testing", "dummy", "placeholder", "tutorial", "example")

# Whole titles (after trimming and lowercasing) that are placeholders.
_PLACEHOLDER_TITLE_RE = re.compile(
    r"^(?:"
    r"test\w*|testing|test[\s_-]*\d+(?: \w+)?|my test(?: \w+)?|"
    r"(?:\w+ )?test (?:dataset|set|data|dandiset)(?: \w+)?|"
    r"dummy(?: \w+)?|asdf+|bla+|zzz+|abc|untitled|unnamed dataset|"
    r"todo:?.*|bids dataset|dataset ?\d*|new dataset|my dataset|"
    r"placeholder.*|sample data(?:set)?|example data(?:set)?|"
    r"user test|neural data|data"
    r")$"
)

# Description phrases that only a test upload would carry.
_DESCRIPTION_JUNK_RE = re.compile(
    r"\b(?:test|dummy|placeholder) (?:dataset|dandiset|upload)\b"
    r"|\bfor testing purposes\b|\bto be deleted\b|\bdo not use\b"
    r"|\btodo: provide (?:a )?description\b",
    re.IGNORECASE,
)

_RETEST_RE = re.compile(r"\btest[\s\-–—_]*retest\b|\bretest\b", re.IGNORECASE)

# Dandisets the find_reuse repo excludes by hand (src/archives/dandi.py,
# TEST_DANDISET_IDS). Kept verbatim so both pipelines agree.
FIND_REUSE_TEST_DANDISETS = frozenset({
    "000027", "000029", "000032", "000033", "000038", "000047", "000068", "000071",
    "000112", "000116", "000118", "000120", "000123", "000124", "000126", "000135",
    "000144", "000145", "000150", "000151", "000154", "000160", "000161", "000162",
    "000164", "000171", "000241", "000299", "000335", "000346", "000349", "000400",
    "000411", "000445", "000470", "000478", "000490", "000529", "000536", "000539",
    "000543", "000544", "000545", "000567", "000712", "000730", "000733", "000881",
    "000942", "000960", "001022", "001049", "001061", "001066", "001083", "001085",
    "001133", "001175", "001698", "001758",
})


def junk_reason(
    source: str,
    dataset_id: Optional[str],
    title: Optional[str],
    description: Optional[str] = None,
    asset_count: Optional[int] = None,
) -> Optional[str]:
    """
    Why this dataset is junk, or None when it looks like a real dataset.

    Rules, in order: no title; title equals the id; curated find_reuse test
    dandiset; empty dandiset (asset_count 0); placeholder title; test/dummy
    keyword in the title; a test-upload phrase in the description.
    """
    ds_id = (dataset_id or "").strip()
    t = " ".join((title or "").split())
    if not t:
        return "no_title"
    tl = t.lower()
    if ds_id and tl == ds_id.lower():
        return "title_is_id"
    if source == "DANDI":
        if ds_id in FIND_REUSE_TEST_DANDISETS:
            return "find_reuse_test_id"
        if asset_count is not None and int(asset_count) == 0:
            return "empty"
    if _PLACEHOLDER_TITLE_RE.match(tl):
        return "placeholder_title"
    hay = _RETEST_RE.sub(" ", tl)
    for kw in TITLE_JUNK_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", hay):
            return f"title_keyword:{kw}"
    d = description or ""
    if d and _DESCRIPTION_JUNK_RE.search(d):
        return "description_phrase"
    return None


def compute_status(
    *,
    mapped: bool,
    papers: Optional[int],
    junk: Optional[str],
) -> Tuple[str, Optional[str]]:
    """(status, reason). A mapped dataset is never excluded: the mapping wins."""
    if mapped:
        return STATUS_MAPPED, None
    if junk:
        return STATUS_EXCLUDED, junk
    if papers == 0:
        return STATUS_NO_PAPER, None
    return STATUS_PENDING, None


def dataset_status_ddl(source: str) -> str:
    """ALTER statements that add the status columns (and DANDI's asset_count)."""
    table = SOURCE_TABLES[source][0]
    stmts = [
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS dataset_status TEXT;",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS dataset_status_reason TEXT;",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_status ON {table}(dataset_status);",
    ]
    if source == "DANDI":
        stmts.insert(0, f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS asset_count INTEGER;")
    return "\n".join(stmts)


def ensure_dataset_status_columns(cursor, source: str) -> Dict[str, int]:
    """Add the columns if missing (skips the lock when they already exist)."""
    return apply_schema_ddl(cursor, dataset_status_ddl(source))


def _table_exists(cursor, table: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL;", (f"public.{table}",))
    row = cursor.fetchone()
    if row is None:
        return False
    if isinstance(row, dict):
        return bool(next(iter(row.values())))
    return bool(row[0])


def _column_exists(cursor, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        );
        """,
        (table, column),
    )
    row = cursor.fetchone()
    if row is None:
        return False
    if isinstance(row, dict):
        return bool(next(iter(row.values())))
    return bool(row[0])


def refresh_dataset_status(cursor, source: str) -> Dict[str, Any]:
    """
    Recompute dataset_status / dataset_status_reason for every row of one archive.

    Returns ``{"source", "total", "updated", "by_status": {...}, "junk_reasons": {...}}``.
    The paper map may not exist yet (ingestion runs before mapping); then nothing
    is mapped. Rows whose status and reason are unchanged are left alone.
    """
    table, map_table, id_col = SOURCE_TABLES[source]
    ensure_dataset_status_columns(cursor, source)

    mapped_expr = "FALSE"
    if _table_exists(cursor, map_table):
        mapped_expr = f"EXISTS (SELECT 1 FROM {map_table} m WHERE m.{id_col} = d.dataset_id)"
    asset_expr = "d.asset_count" if source == "DANDI" and _column_exists(cursor, table, "asset_count") else "NULL"

    cursor.execute(
        f"""
        SELECT d.dataset_id, d.title, d.description, d.papers,
               d.dataset_status, d.dataset_status_reason, {asset_expr} AS asset_count,
               {mapped_expr} AS mapped
        FROM {table} d;
        """
    )
    rows = cursor.fetchall()

    by_status: Dict[str, int] = {s: 0 for s in ALL_STATUSES}
    junk_reasons: Dict[str, int] = {}
    changes: List[Tuple[str, Optional[str], str]] = []
    for row in rows:
        if isinstance(row, dict):
            ds_id, title, description = row["dataset_id"], row["title"], row["description"]
            papers, old_status, old_reason = row["papers"], row["dataset_status"], row["dataset_status_reason"]
            asset_count, mapped = row["asset_count"], row["mapped"]
        else:
            ds_id, title, description, papers, old_status, old_reason, asset_count, mapped = row
        junk = junk_reason(source, ds_id, title, description, asset_count)
        status, reason = compute_status(mapped=bool(mapped), papers=papers, junk=junk)
        by_status[status] += 1
        if reason:
            junk_reasons[reason] = junk_reasons.get(reason, 0) + 1
        if status != old_status or reason != old_reason:
            changes.append((status, reason, ds_id))

    if changes:
        cursor.executemany(
            f"UPDATE {table} SET dataset_status = %s, dataset_status_reason = %s WHERE dataset_id = %s;",
            changes,
        )
    result = {
        "source": source,
        "total": len(rows),
        "updated": len(changes),
        "by_status": by_status,
        "junk_reasons": junk_reasons,
    }
    logger.info(
        "%s dataset_status refreshed: %d rows, %d changed, %s, junk reasons %s",
        source, len(rows), len(changes), by_status, junk_reasons,
    )
    return result


def refresh_all_dataset_status(cursor, sources: Iterable[str] = SOURCE_TABLES) -> Dict[str, Any]:
    """refresh_dataset_status for every archive whose dataset table exists."""
    out: Dict[str, Any] = {}
    for source in sources:
        if _table_exists(cursor, SOURCE_TABLES[source][0]):
            out[source] = refresh_dataset_status(cursor, source)
    return out
