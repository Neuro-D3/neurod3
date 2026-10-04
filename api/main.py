"""
Backend API service for NeuroD3 dataset discovery.
Provides REST endpoints to fetch neuroscience datasets from PostgreSQL.
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, Dict, List, Optional, Tuple
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import os
import sys
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone
import html
import logging
import re
import unicodedata

# Add dags directory to path so we can import shared utilities
dags_path = Path(__file__).parent.parent / "dags"
if str(dags_path) not in sys.path:
    sys.path.insert(0, str(dags_path))

try:
    # Prefer the shared Airflow DAGs utility when available (local dev / Airflow environment)
    from utils.database import create_unified_datasets_view
except ModuleNotFoundError:
    # In the Docker API container we may not have the Airflow dags package mounted.
    # Provide a safe no-op fallback so the API can still start and serve core endpoints.
    def create_unified_datasets_view(cursor):  # type: ignore[override]
        """
        Fallback stub for create_unified_datasets_view.
        This is used when the shared Airflow utils module is not available.
        It reports that no view was created so callers can handle it gracefully.
        """
        return {
            "view_created": False,
            "total_rows": 0,
            "rows_by_source": {},
            "dandi_table_exists": False,
            "neuro_table_exists": False,
        }

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NeuroD3 API", version="1.0.0")

# Allowed filter values
# The four archives with ingestion + paper-mapping pipelines. Kaggle and
# PhysioNet seed rows still exist in neuroscience_datasets but are left out of
# the unified view (utils/database.py) and are not valid filters here.
ALLOWED_SOURCES = {"CRCNS", "DANDI", "OpenNeuro", "SPARC"}
ALLOWED_PAPER_MAPPING_SOURCES = {"CRCNS", "DANDI", "OpenNeuro", "SPARC"}

# CORS configuration to allow the frontend to access the API.
# Local/dev origins are always allowed; cloud deployments add their frontend
# origin(s) via the ALLOWED_ORIGINS env var (comma-separated). On Cloud Run the
# frontend and API are on different *.run.app origins, so the frontend's URL must
# be listed here for browser requests to succeed.
_DEFAULT_ORIGINS = ["http://localhost:3000", "http://frontend:3000"]
_extra_origins = [
    o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_DEFAULT_ORIGINS + _extra_origins,
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept"],
)

# Database configuration
DB_CONNINFO = (
    f"host={os.getenv('DB_HOST', 'postgres')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME', 'dag_data')} "
    f"user={os.getenv('DB_USER', 'airflow')} "
    f"password={os.getenv('DB_PASSWORD', 'airflow')}"
)

# Build version stamp (git SHA baked at image build), surfaced via /api/health.
APP_VERSION = os.getenv("APP_VERSION", "dev")


@contextmanager
def get_db_connection():
    """Context manager for database connections.

    Only connect() failures are reported as "connection failed". Query errors
    raised while the connection is in use propagate to the caller so they surface
    accurately (e.g. "Database query failed: relation ... does not exist") instead
    of being mislabeled as a connection failure.
    """
    try:
        conn = psycopg.connect(DB_CONNINFO)
    except psycopg.Error as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        yield conn
    finally:
        conn.close()


def _validate_paper_mapping_source(source: Optional[str]) -> Optional[str]:
    if source is None:
        return None
    if source not in ALLOWED_PAPER_MAPPING_SOURCES:
        raise HTTPException(status_code=400, detail=f"Invalid paper mapping source: {source}")
    return source


# ORDER BY for citation lists (alias `cc` = citation_classifications, `ce` =
# citation_edges). Labelled pairs come first so a capped list shows what the
# classifier found, not only the newest (usually unclassified) citing papers.
CITATION_DISPLAY_ORDER_SQL = """
    CASE
        WHEN cc.classification = 'REUSE' THEN 0
        WHEN cc.classification = 'PRIMARY' THEN 1
        WHEN cc.classification = 'MENTION' THEN 2
        WHEN cc.classification = 'NEITHER' THEN 3
        WHEN cc.status IN ('error', 'no_full_text') THEN 4
        ELSE 5
    END
"""


# Inline markup publishers leave in paper titles (JATS, HTML, MathML); the same
# list as airflow/dags/utils/titles.py, which cleans titles as they are stored.
# Titles stored before that are cleaned here, as they are served.
_TITLE_MARKUP_TAG = re.compile(
    r"</?(?:i|b|em|strong|u|sup|sub|scp|sc|span|italic|bold|small|underline"
    r"|inline-formula|tex-math|alternatives|mml:[a-z]+)\b[^<>]*>",
    re.IGNORECASE,
)
_PAPER_TITLE_KEYS: Tuple[str, ...] = ("paper_title", "primary_paper_title", "citing_paper_title", "title")


def _clean_title(title: Any) -> Any:
    """A paper title without markup: entities decoded, known inline tags dropped (text kept)."""
    if not isinstance(title, str):
        return title
    text = re.sub(r"\s+", " ", _TITLE_MARKUP_TAG.sub("", html.unescape(title))).strip()
    return text or None


def _clean_paper_titles(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Clean the paper-title fields of API rows in place (dataset titles are left alone)."""
    for row in rows:
        for key in _PAPER_TITLE_KEYS:
            if key in row:
                row[key] = _clean_title(row[key])
    return rows


def _paper_mapping_relation_exists(cursor, relation_name: str, relation_type: str = "table") -> bool:
    if relation_type == "view":
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.views
                WHERE table_schema = 'public'
                  AND table_name = %s
            ) AS exists;
            """,
            (relation_name,),
        )
    else:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = %s
            ) AS exists;
            """,
            (relation_name,),
        )
    return bool(cursor.fetchone()["exists"])


def _ensure_paper_mapping_tables(cursor) -> None:
    required_tables = [
        "papers",
        "dandi_paper_map",
        "openneuro_paper_map",
        "dandi_paper_citations",
        "openneuro_paper_citations",
        "dandi_paper_citation_classifications",
        "openneuro_paper_citation_classifications",
    ]
    missing = [name for name in required_tables if not _paper_mapping_relation_exists(cursor, name, "table")]
    if missing:
        raise HTTPException(
            status_code=503,
            detail=(
                "Paper mapping tables are missing. Run the DANDI/OpenNeuro paper mapping DAGs first. "
                f"Missing: {', '.join(missing)}"
            ),
        )


# Columns the whole-paper classifier writes (airflow/dags/utils/database.py,
# ensure_paper_reuse_classification_columns) beyond the original set, with the
# type each should have when the table predates them. Only those the API and
# site surface; the DAG stores more (usage, truncation, quote_warnings).
CLASSIFICATION_EXTRA_COLUMNS: List[Tuple[str, str]] = [
    ("prompt_version", "integer"),
    ("mode", "text"),
    ("reuse_type", "text"),
    ("reuse_type_other", "text"),
    ("reused_modalities", "jsonb"),
    ("reused_dandi_hosted", "boolean"),
    ("evidence_quotes", "jsonb"),
    ("source_quotes", "jsonb"),
    ("hallucinated_quote_count", "integer"),
    ("error_kind", "text"),
    ("provider", "text"),
]

# Labels that mean "this paper reused the dataset's data". (The excerpt-based
# classifier's SECONDARY label was retired once every row had been
# reclassified under the whole-paper scheme.)
REUSE_CLASSIFICATIONS: Tuple[str, ...] = ("REUSE",)
REUSE_CLASSIFICATIONS_SQL = "(" + ", ".join(f"'{c}'" for c in REUSE_CLASSIFICATIONS) + ")"


def _table_columns(cursor, table_name: str) -> set:
    """Column names of a public table (empty set when the table is missing)."""
    if cursor is None:
        return set()
    cursor.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s;
        """,
        (table_name,),
    )
    return {r["column_name"] for r in cursor.fetchall()}


def _classification_extra_columns_sql(cursor, table_name: str, alias: str = "c") -> str:
    """
    SELECT-list fragment for CLASSIFICATION_EXTRA_COLUMNS on one source table.

    A deployment whose DAGs have not run since the columns were introduced
    still has the old table shape; selecting the columns outright would make
    every paper-mapping endpoint fail. Missing columns are selected as typed
    NULLs so the UNION stays type-consistent across sources.
    """
    present = _table_columns(cursor, table_name)
    parts = []
    for column, col_type in CLASSIFICATION_EXTRA_COLUMNS:
        if column in present:
            parts.append(f"{alias}.{column}")
        else:
            parts.append(f"NULL::{col_type} AS {column}")
    return ",\n            ".join(parts)


# (classification table, dataset id column, unified_datasets.source label)
_CLASSIFICATION_TABLES: List[Tuple[str, str, str]] = [
    ("dandi_paper_citation_classifications", "dandi_id", "DANDI"),
    ("openneuro_paper_citation_classifications", "openneuro_id", "OpenNeuro"),
    ("crcns_paper_citation_classifications", "crcns_id", "CRCNS"),
    ("sparc_paper_citation_classifications", "sparc_id", "SPARC"),
]


# Citing papers are counted as *works*: a preprint and its published version,
# and eLife's version DOIs (10.7554/eLife.84630 and ...84630.3), are one work.
# A work is its normalized title (markup, entities, punctuation and case
# removed). Titles shorter than WORK_KEY_MIN_CHARS after that are too generic to
# merge on, so those papers stay their own work. Defined once, in SQL, so every
# count and list agrees.
WORK_KEY_MIN_CHARS = 16
_WORK_KEY_TAG_RE = (
    r"</?(i|b|em|strong|u|sup|sub|scp|sc|span|italic|bold|small|underline"
    r"|inline-formula|tex-math|alternatives|mml:[a-z]+)\y[^<>]*>"
)


def work_key_sql(title_expr: str, doi_expr: str) -> str:
    """SQL expression for the work a paper belongs to: 't:<title>' or, for short titles, 'd:<doi>'."""
    norm = (
        "lower(regexp_replace(regexp_replace(regexp_replace("
        f"COALESCE({title_expr}, ''), '{_WORK_KEY_TAG_RE}', '', 'gi'), "
        "'&#?[a-z0-9]+;', '', 'gi'), '[^a-zA-Z0-9]+', '', 'g'))"
    )
    return f"(CASE WHEN length({norm}) >= {WORK_KEY_MIN_CHARS} THEN 't:' || {norm} ELSE 'd:' || lower({doi_expr}) END)"


# Preprint servers' DOIs: bioRxiv/medRxiv (10.1101 with a numeric suffix; other
# 10.1101 DOIs are Cold Spring Harbor journals), bioRxiv from 2026 (10.64898),
# arXiv, Research Square, SSRN, PsyArXiv, OSF Preprints, SocArXiv,
# Preprints.org, Authorea, TechRxiv, ChemRxiv, engrXiv, ESS Open Archive.
_PREPRINT_DOI = re.compile(
    r"^10\.(?:1101/\d|64898/|48550/|21203/|2139/|31234/|31219/|31235/|20944/|22541/"
    r"|36227/|26434/|31224/|1002/essoar\.)",
    re.IGNORECASE,
)


def is_preprint_doi(doi: Any) -> bool:
    return isinstance(doi, str) and bool(_PREPRINT_DOI.match(doi))


def pick_published_version(dois: List[str]) -> str:
    """
    The version a work is shown as: a published DOI over a preprint's, and an
    umbrella DOI (eLife's 10.7554/eLife.84630) over its numbered versions.
    """
    def rank(doi: str) -> Tuple[bool, bool, str]:
        lowered = doi.lower()
        umbrella = any(other.lower().startswith(lowered + ".") for other in dois if other != doi)
        return (is_preprint_doi(doi), not umbrella, lowered)

    return min(dois, key=rank)


def _annotate_versions(
    rows: List[Dict[str, Any]], *, doi_key: str, work_key: str, date_key: str, prefix: str = ""
) -> List[Dict[str, Any]]:
    """
    Mark the versions of each work among `rows` so the site can list a work
    once: whether each paper is a preprint (`{prefix}is_preprint`), the DOI the
    work is shown as (`{prefix}work_doi`) and, when it has several versions,
    all of them with their dates (`{prefix}work_versions`, oldest first).
    """
    versions_by_work: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        doi = row.get(doi_key)
        if doi:
            row[work_key] = row.get(work_key) or f"d:{doi.lower()}"
            versions_by_work.setdefault(row[work_key], {})[doi] = row.get(date_key)
    for row in rows:
        doi = row.get(doi_key)
        if not doi:
            continue
        versions = versions_by_work[row[work_key]]
        row[f"{prefix}is_preprint"] = is_preprint_doi(doi)
        row[f"{prefix}work_doi"] = pick_published_version(list(versions))
        row[f"{prefix}work_versions"] = [
            {"doi": d, "is_preprint": is_preprint_doi(d), "publication_date": versions[d]}
            for d in sorted(versions, key=lambda d: (versions[d] is None, versions[d] or "", d))
        ] if len(versions) > 1 else []
    return rows


def _reuse_count_subquery(cursor, dataset_alias: str = "d") -> str:
    """
    SQL expression: citing works classified as reuse for one dataset.

    One correlated COUNT per source table that exists, summed; "0" when none
    exist yet. Counts the labels in REUSE_CLASSIFICATIONS, and a preprint and
    its published version once (see work_key_sql). Each count is tied to its
    archive via `{dataset_alias}.source`, since dataset ids are only unique
    within an archive.
    """
    parts = []
    for table, id_col, source in _CLASSIFICATION_TABLES:
        if _paper_mapping_relation_exists(cursor, table):
            parts.append(
                f"(CASE WHEN {dataset_alias}.source = '{source}' THEN "
                f"COALESCE((SELECT COUNT(DISTINCT {work_key_sql('p.title', 'c.citing_paper_doi')})::int "
                f"FROM {table} c LEFT JOIN papers p ON p.paper_doi = c.citing_paper_doi "
                f"WHERE c.{id_col} = {dataset_alias}.dataset_id "
                f"AND c.classification IN {REUSE_CLASSIFICATIONS_SQL}), 0) ELSE 0 END)"
            )
    return " + ".join(parts) if parts else "0"


def _paper_mapping_ctes(cursor=None) -> str:
    """Build the WITH … CTEs that union per-source paper-mapping tables.

    If a cursor is provided, CRCNS and SPARC branches are appended only when
    the corresponding tables exist (their ingestion / paper-mapping DAGs may
    not yet have run on every deployment). When no cursor is supplied we
    omit them to preserve the original DANDI/OpenNeuro-only behavior.
    """
    has_crcns_dataset = bool(cursor) and _paper_mapping_relation_exists(cursor, "crcns_dataset")
    has_crcns_map = bool(cursor) and _paper_mapping_relation_exists(cursor, "crcns_paper_map")
    has_crcns_citations = bool(cursor) and _paper_mapping_relation_exists(cursor, "crcns_paper_citations")
    has_crcns_classifications = bool(cursor) and _paper_mapping_relation_exists(
        cursor, "crcns_paper_citation_classifications"
    )
    has_sparc_dataset = bool(cursor) and _paper_mapping_relation_exists(cursor, "sparc_dataset")
    has_sparc_map = bool(cursor) and _paper_mapping_relation_exists(cursor, "sparc_paper_map")
    has_sparc_citations = bool(cursor) and _paper_mapping_relation_exists(cursor, "sparc_paper_citations")
    has_sparc_classifications = bool(cursor) and _paper_mapping_relation_exists(
        cursor, "sparc_paper_citation_classifications"
    )

    # Whole-paper classification columns, selected as typed NULLs where a
    # table has not been migrated yet (see _classification_extra_columns_sql).
    dandi_cls_extra = _classification_extra_columns_sql(cursor, "dandi_paper_citation_classifications")
    openneuro_cls_extra = _classification_extra_columns_sql(cursor, "openneuro_paper_citation_classifications")
    crcns_cls_extra = _classification_extra_columns_sql(cursor, "crcns_paper_citation_classifications")
    sparc_cls_extra = _classification_extra_columns_sql(cursor, "sparc_paper_citation_classifications")

    crcns_dataset_branch = """
        UNION ALL
        SELECT
            'CRCNS'::text AS source,
            d.dataset_id::text AS dataset_id,
            d.title AS dataset_title,
            d.description AS dataset_description,
            d.modality AS modality,
            d.papers AS papers_count,
            d.created_at,
            d.updated_at,
            d.url
        FROM crcns_dataset d
    """ if has_crcns_dataset else ""

    crcns_map_branch = """
        UNION ALL
        SELECT
            'CRCNS'::text AS source,
            m.crcns_id::text AS dataset_id,
            m.crcns_title AS dataset_title,
            m.paper_doi,
            m.doi_source,
            m.relation_type,
            m.resolved_at,
            m.run_id
        FROM crcns_paper_map m
    """ if has_crcns_map else ""

    crcns_citations_branch = """
        UNION ALL
        SELECT
            'CRCNS'::text AS source,
            c.crcns_id::text AS dataset_id,
            c.primary_paper_doi,
            c.citing_paper_doi,
            c.matched_primary_paper_doi,
            c.matched_primary_openalex_id,
            c.citation_source,
            c.citing_publication_date,
            c.citation_contexts,
            c.contexts_extracted_at,
            c.resolved_at,
            c.run_id
        FROM crcns_paper_citations c
    """ if has_crcns_citations else ""

    crcns_classifications_branch = f"""
        UNION ALL
        SELECT
            'CRCNS'::text AS source,
            c.crcns_id::text AS dataset_id,
            c.primary_paper_doi,
            c.citing_paper_doi,
            c.classification,
            c.same_lab,
            c.confidence,
            c.status,
            c.same_lab_confidence,
            c.source_archive,
            c.reasoning,
            c.classification_model,
            c.classified_at,
            c.run_id,
            {crcns_cls_extra}
        FROM crcns_paper_citation_classifications c
    """ if has_crcns_classifications else ""

    sparc_dataset_branch = """
        UNION ALL
        SELECT
            'SPARC'::text AS source,
            d.dataset_id::text AS dataset_id,
            d.title AS dataset_title,
            d.description AS dataset_description,
            d.modality AS modality,
            d.papers AS papers_count,
            d.created_at,
            d.updated_at,
            d.url
        FROM sparc_dataset d
    """ if has_sparc_dataset else ""

    sparc_map_branch = """
        UNION ALL
        SELECT
            'SPARC'::text AS source,
            m.sparc_id::text AS dataset_id,
            m.sparc_title AS dataset_title,
            m.paper_doi,
            m.doi_source,
            m.relation_type,
            m.resolved_at,
            m.run_id
        FROM sparc_paper_map m
    """ if has_sparc_map else ""

    sparc_citations_branch = """
        UNION ALL
        SELECT
            'SPARC'::text AS source,
            c.sparc_id::text AS dataset_id,
            c.primary_paper_doi,
            c.citing_paper_doi,
            c.matched_primary_paper_doi,
            c.matched_primary_openalex_id,
            c.citation_source,
            c.citing_publication_date,
            c.citation_contexts,
            c.contexts_extracted_at,
            c.resolved_at,
            c.run_id
        FROM sparc_paper_citations c
    """ if has_sparc_citations else ""

    sparc_classifications_branch = f"""
        UNION ALL
        SELECT
            'SPARC'::text AS source,
            c.sparc_id::text AS dataset_id,
            c.primary_paper_doi,
            c.citing_paper_doi,
            c.classification,
            c.same_lab,
            c.confidence,
            c.status,
            c.same_lab_confidence,
            c.source_archive,
            c.reasoning,
            c.classification_model,
            c.classified_at,
            c.run_id,
            {sparc_cls_extra}
        FROM sparc_paper_citation_classifications c
    """ if has_sparc_classifications else ""

    return f"""
    WITH dataset_base AS (
        SELECT
            'DANDI'::text AS source,
            d.dataset_id::text AS dataset_id,
            d.title AS dataset_title,
            d.description AS dataset_description,
            d.modality AS modality,
            d.papers AS papers_count,
            d.created_at,
            d.updated_at,
            d.url
        FROM dandi_dataset d
        UNION ALL
        SELECT
            'OpenNeuro'::text AS source,
            d.dataset_id::text AS dataset_id,
            d.title AS dataset_title,
            d.description AS dataset_description,
            d.modality AS modality,
            d.papers AS papers_count,
            d.created_at,
            d.updated_at,
            d.url
        FROM openneuro_dataset d
        {crcns_dataset_branch}
        {sparc_dataset_branch}
    ),
    dataset_map AS (
        SELECT
            'DANDI'::text AS source,
            m.dandi_id::text AS dataset_id,
            m.dandi_title AS dataset_title,
            m.paper_doi,
            m.doi_source,
            m.relation_type,
            m.resolved_at,
            m.run_id
        FROM dandi_paper_map m
        UNION ALL
        SELECT
            'OpenNeuro'::text AS source,
            m.openneuro_id::text AS dataset_id,
            m.openneuro_title AS dataset_title,
            m.paper_doi,
            m.doi_source,
            m.relation_type,
            m.resolved_at,
            m.run_id
        FROM openneuro_paper_map m
        {crcns_map_branch}
        {sparc_map_branch}
    ),
    citation_edges AS (
        SELECT
            'DANDI'::text AS source,
            c.dandi_id::text AS dataset_id,
            c.primary_paper_doi,
            c.citing_paper_doi,
            c.matched_primary_paper_doi,
            c.matched_primary_openalex_id,
            c.citation_source,
            c.citing_publication_date,
            c.citation_contexts,
            c.contexts_extracted_at,
            c.resolved_at,
            c.run_id
        FROM dandi_paper_citations c
        UNION ALL
        SELECT
            'OpenNeuro'::text AS source,
            c.openneuro_id::text AS dataset_id,
            c.primary_paper_doi,
            c.citing_paper_doi,
            c.matched_primary_paper_doi,
            c.matched_primary_openalex_id,
            c.citation_source,
            c.citing_publication_date,
            c.citation_contexts,
            c.contexts_extracted_at,
            c.resolved_at,
            c.run_id
        FROM openneuro_paper_citations c
        {crcns_citations_branch}
        {sparc_citations_branch}
    ),
    citation_classifications AS (
        SELECT
            'DANDI'::text AS source,
            c.dandi_id::text AS dataset_id,
            c.primary_paper_doi,
            c.citing_paper_doi,
            c.classification,
            c.same_lab,
            c.confidence,
            c.status,
            c.same_lab_confidence,
            c.source_archive,
            c.reasoning,
            c.classification_model,
            c.classified_at,
            c.run_id,
            {dandi_cls_extra}
        FROM dandi_paper_citation_classifications c
        UNION ALL
        SELECT
            'OpenNeuro'::text AS source,
            c.openneuro_id::text AS dataset_id,
            c.primary_paper_doi,
            c.citing_paper_doi,
            c.classification,
            c.same_lab,
            c.confidence,
            c.status,
            c.same_lab_confidence,
            c.source_archive,
            c.reasoning,
            c.classification_model,
            c.classified_at,
            c.run_id,
            {openneuro_cls_extra}
        FROM openneuro_paper_citation_classifications c
        {crcns_classifications_branch}
        {sparc_classifications_branch}
    )
    """


def _paper_mapping_filter_sql(
    *,
    source: Optional[str] = None,
    search: Optional[str] = None,
) -> tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    if source:
        clauses.append("db.source = %s")
        params.append(source)
    if search:
        clauses.append(
            "(db.dataset_title ILIKE %s OR db.dataset_id ILIKE %s OR COALESCE(db.dataset_description, '') ILIKE %s)"
        )
        like = f"%{search}%"
        params.extend([like, like, like])
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def _count_contexts_expr(alias: str) -> str:
    return (
        f"CASE WHEN jsonb_typeof({alias}.citation_contexts) = 'array' "
        f"THEN jsonb_array_length({alias}.citation_contexts) ELSE 0 END"
    )


@app.get("/")
async def root():
    """API health check endpoint."""
    return {"status": "ok", "message": "NeuroD3 API is running"}


@app.get("/api/health")
async def health_check():
    """Health check endpoint that verifies database connectivity."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
                # Check if unified_datasets view exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.views 
                        WHERE table_schema = 'public' 
                        AND table_name = 'unified_datasets'
                    );
                """)
                view_row = cursor.fetchone()
                view_exists = bool(view_row[0])
                
                if view_exists:
                    cursor.execute("SELECT COUNT(*) FROM unified_datasets")
                    view_count = cursor.fetchone()[0]
                    return {
                        "status": "healthy",
                        "database": "connected",
                        "version": APP_VERSION,
                        "unified_datasets_view": "exists",
                        "view_row_count": view_count
                    }
                else:
                    return {
                        "status": "healthy",
                        "database": "connected",
                        "version": APP_VERSION,
                        "unified_datasets_view": "missing"
                    }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail={"status": "unhealthy", "database": "disconnected", "error": str(e)}
        )


def _dataset_visible_sql(alias: str, has_status_column: bool) -> str:
    """
    ` AND <alias>.dataset_status <> 'excluded'` once the dataset tables carry the
    column (utils/dataset_status.py), else "". Junk datasets (test / placeholder /
    empty uploads) are ingested, so the mapping DAGs can record them, but never
    shown or counted on the site.
    """
    if not has_status_column:
        return ""
    prefix = f"{alias}." if alias else ""
    return f" AND COALESCE({prefix}dataset_status, '') <> 'excluded'"


def _relation_has_column(cursor, relation: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        ) AS exists;
        """,
        (relation, column),
    )
    row = cursor.fetchone()
    return bool(row and row["exists"])


def _dataset_order_sql(sort_by: Optional[str], sort_order: Optional[str], reuse_subquery: str) -> str:
    """
    ORDER BY clause for GET /api/datasets. Ties are broken deterministically;
    datasets with the same reuse count (most have none) stay newest first.
    Unknown sorts are a 400.
    """
    sort_by_norm = (sort_by or "published").strip().lower()
    sort_order_norm = (sort_order or "desc").strip().lower()
    if sort_order_norm not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail=f"Invalid sort_order: {sort_order}")

    sort_column_by_key = {
        "published": "d.created_at",
        "papers": f"(COALESCE(d.papers, 0) + ({reuse_subquery}))",
        # Citing works classified as reuse (a preprint and its published version once).
        "reuse": f"({reuse_subquery})",
        "title": "d.title",
        "id": "d.dataset_id",
        "source": "d.source",
        "modality": "d.modality",
    }
    sort_col = sort_column_by_key.get(sort_by_norm)
    if not sort_col:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by: {sort_by}")

    tie_breakers = "d.title ASC, d.dataset_id ASC"
    if sort_by_norm == "reuse":
        tie_breakers = f"d.created_at DESC NULLS LAST, {tie_breakers}"
    return f"{sort_col} {sort_order_norm.upper()} NULLS LAST, {tie_breakers}"


@app.get("/api/datasets")
async def get_datasets(
    source: Optional[str] = Query(None, description="Filter by source (CRCNS, DANDI, OpenNeuro, SPARC)"),
    modality: Optional[str] = Query(None, description="Filter by modality (comma-separated for AND)"),
    search: Optional[str] = Query(None, description="Search in title and description"),
    sort_by: str = Query("published", description="Sort column (published, papers, reuse, title, id, source, modality)"),
    sort_order: str = Query("desc", description="Sort order (asc, desc)"),
    limit: int = Query(25, ge=1, le=200, description="Max number of datasets to return"),
    offset: int = Query(0, ge=0, description="Number of datasets to skip"),
):
    """
    Fetch neuroscience datasets from the database.

    Supports filtering by:
    - source: Dataset source platform
    - modality: Data modality (fMRI, EEG, etc.)
    - search: Search term for title/description
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                # Check if unified_datasets view exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.views 
                        WHERE table_schema = 'public' 
                        AND table_name = 'unified_datasets'
                    );
                """)
                view_row = cursor.fetchone()
                view_exists = view_row["exists"]
                
                # Check if neuroscience_datasets table exists (fallback target)
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'neuroscience_datasets'
                    );
                """)
                neuro_row = cursor.fetchone()
                neuro_table_exists = neuro_row["exists"]
                
                if not view_exists and not neuro_table_exists:
                    # Database is missing both view and base table (likely fresh or reset DB)
                    raise HTTPException(
                        status_code=503,
                        detail="No dataset tables or view found. Run the populate_neuroscience_datasets and dandi_ingestion DAGs or POST /api/refresh-view after tables exist."
                    )
                
                # Build dynamic query based on filters
                if view_exists:
                    logger.info("Using unified_datasets view for query")
                    table_name = "unified_datasets"
                else:
                    # Fallback to neuroscience_datasets table if view doesn't exist
                    logger.warning("unified_datasets view does not exist, falling back to neuroscience_datasets table")
                    table_name = "neuroscience_datasets"

                # Check which optional columns exist on the dataset table/view
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                      AND column_name IN ('authors', 'num_subjects', 'created_at_precision', 'dataset_status')
                """, (table_name,))
                ds_opt_cols = {r["column_name"] for r in cursor.fetchall()}
                # Junk datasets (dataset_status = 'excluded') stay out of the list and its count.
                visible_sql = _dataset_visible_sql("d", "dataset_status" in ds_opt_cols)
                authors_expr = "authors," if "authors" in ds_opt_cols else "NULL::jsonb AS authors,"
                num_subjects_expr = "num_subjects," if "num_subjects" in ds_opt_cols else "NULL::integer AS num_subjects,"
                # "year" when only the publication year is known (CRCNS), so the page shows "2011".
                precision_expr = (
                    "d.created_at_precision," if "created_at_precision" in ds_opt_cols
                    else "NULL::text AS created_at_precision,"
                )

                # reuse_count: citing papers the LLM classified as reusing the dataset's
                # data (see REUSE_CLASSIFICATIONS). The per-source classification
                # tables only exist after the paper-mapping DAGs have run, so each
                # branch is included only when its table exists — otherwise the whole
                # query fails with UndefinedTable and the endpoint 500s.
                reuse_subquery = _reuse_count_subquery(cursor)

                base_select = f"""
                    SELECT
                        d.source,
                        d.dataset_id as id,
                        d.title,
                        d.modality,
                        d.papers,
                        d.url,
                        d.description,
                        {authors_expr.replace('authors', 'd.authors') if 'authors' in ds_opt_cols else authors_expr}
                        {num_subjects_expr.replace('num_subjects', 'd.num_subjects') if 'num_subjects' in ds_opt_cols else num_subjects_expr}
                        d.created_at,
                        {precision_expr}
                        d.updated_at,
                        ({reuse_subquery}) AS reuse_count
                    FROM {table_name} d
                    WHERE 1=1{visible_sql}
                """
                base_count = f"SELECT COUNT(*) as total FROM {table_name} d WHERE 1=1{visible_sql}"
                filters = []
                params = []

                if source:
                    if source not in ALLOWED_SOURCES:
                        raise HTTPException(status_code=400, detail=f"Invalid source: {source}")
                    filters.append("d.source = %s")
                    params.append(source)

                if modality:
                    modalities = [m.strip() for m in modality.split(",") if m.strip()]
                    for m in modalities:
                        filters.append("d.modality ILIKE %s")
                        params.append(f"%{m}%")

                if search:
                    filters.append("(d.title ILIKE %s OR d.description ILIKE %s OR d.authors::text ILIKE %s)")
                    search_pattern = f"%{search}%"
                    params.extend([search_pattern, search_pattern, search_pattern])

                filter_sql = f" AND {' AND '.join(filters)}" if filters else ""

                # Server-side ordering (applies before pagination).
                order_sql = _dataset_order_sql(sort_by, sort_order, reuse_subquery)
                query = f"{base_select}{filter_sql} ORDER BY {order_sql} LIMIT %s OFFSET %s"
                count_query = f"{base_count}{filter_sql}"

                cursor.execute(count_query, params)
                total = cursor.fetchone()["total"]

                cursor.execute(query, params + [limit, offset])
                datasets = cursor.fetchall()

                # Convert to list of dicts
                result = [dict(row) for row in datasets]

                # Attach paper titles/DOIs (best-effort).
                # This keeps the main dataset query simple and adds at most one extra query per source per request.
                for r in result:
                    r["paper_dois"] = None
                    r["paper_titles"] = None

                dandi_ids = [r["id"] for r in result if r.get("source") == "DANDI" and r.get("id")]
                openneuro_ids = [r["id"] for r in result if r.get("source") == "OpenNeuro" and r.get("id")]

                if dandi_ids:
                    try:
                        cursor.execute(
                            """
                            SELECT dandi_id, paper_dois, paper_titles
                            FROM dandi_dataset_papers
                            WHERE dandi_id = ANY(%s);
                            """,
                            (dandi_ids,),
                        )
                        rows = cursor.fetchall()
                        papers_by_id = {
                            row["dandi_id"]: {"paper_dois": row["paper_dois"], "paper_titles": row["paper_titles"]}
                            for row in rows
                        }
                        for r in result:
                            if r.get("source") == "DANDI":
                                entry = papers_by_id.get(r.get("id"), {})
                                r["paper_dois"] = entry.get("paper_dois")
                                r["paper_titles"] = entry.get("paper_titles")
                    except Exception as e:
                        # View may not exist yet; fail soft.
                        logger.warning("Could not attach paper_titles from dandi_dataset_papers: %s", e)

                if openneuro_ids:
                    try:
                        cursor.execute(
                            """
                            SELECT openneuro_id, paper_dois, paper_titles
                            FROM openneuro_dataset_papers
                            WHERE openneuro_id = ANY(%s);
                            """,
                            (openneuro_ids,),
                        )
                        rows = cursor.fetchall()
                        papers_by_id = {
                            row["openneuro_id"]: {"paper_dois": row["paper_dois"], "paper_titles": row["paper_titles"]}
                            for row in rows
                        }
                        for r in result:
                            if r.get("source") == "OpenNeuro":
                                entry = papers_by_id.get(r.get("id"), {})
                                r["paper_dois"] = entry.get("paper_dois")
                                r["paper_titles"] = entry.get("paper_titles")
                    except Exception as e:
                        # View may not exist yet; fail soft.
                        logger.warning("Could not attach paper_titles from openneuro_dataset_papers: %s", e)

                return {
                    "datasets": result,
                    "count": total
                }

    except HTTPException:
        raise  # e.g. 400 for an invalid sort_by; the catch-all below would turn it into a 500
    except psycopg.Error as e:
        logger.exception("Database query error")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.exception("Unexpected error in /api/datasets")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/datasets/stats")
async def get_dataset_stats(
    source: Optional[str] = Query(None, description="Facet by source (applies to modality counts)"),
    modality: Optional[str] = Query(None, description="Facet by modality (applies to source counts)"),
    search: Optional[str] = Query(None, description="Search in title and description"),
):
    """
    Get statistics about datasets in the database.
    Returns counts by source and modality.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                # Check if unified_datasets view exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.views 
                        WHERE table_schema = 'public' 
                        AND table_name = 'unified_datasets'
                    );
                """)
                view_exists = cursor.fetchone()['exists']
                
                # Whitelist of allowed table/view names for safety
                ALLOWED_TABLE_NAMES = {"unified_datasets", "neuroscience_datasets", "dandi_dataset"}
                
                table_name = "unified_datasets" if view_exists else "neuroscience_datasets"
                if not view_exists:
                    logger.warning("unified_datasets view does not exist, using neuroscience_datasets table for stats")
                
                # Validate table name against whitelist
                if table_name not in ALLOWED_TABLE_NAMES:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid table name: {table_name}"
                    )
                
                # Ensure the chosen table/view actually exists before querying
                cursor.execute("""
                    SELECT (
                        EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        ) OR EXISTS (
                            SELECT FROM information_schema.views 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    ) AS exists;
                """, (table_name, table_name))
                target_exists = cursor.fetchone()["exists"]
                if not target_exists:
                    raise HTTPException(
                        status_code=503,
                        detail="Dataset view/table not found. Run the populate_neuroscience_datasets and dandi_ingestion DAGs or POST /api/refresh-view after tables exist."
                    )
                
                # Use psycopg.sql.Identifier() for safe table name construction
                table_identifier = sql.Identifier(table_name)

                # Junk datasets (dataset_status = 'excluded') are left out of every count.
                visible_clauses: List[Any] = []
                if _relation_has_column(cursor, table_name, "dataset_status"):
                    visible_clauses.append(sql.SQL("COALESCE(dataset_status, '') <> 'excluded'"))

                # Parse/validate incoming filters (used for facets/total)
                if source and source not in ALLOWED_SOURCES:
                    raise HTTPException(status_code=400, detail=f"Invalid source: {source}")
                modalities = []
                if modality:
                    modalities = [m.strip() for m in modality.split(",") if m.strip()]
                search_pattern = f"%{search}%" if search else None

                # Facet counts
                # - by_source: apply modality + search filters (but not source)
                # - by_modality: apply source + search filters (but not modality)
                by_source_params = []
                by_source_clauses = list(visible_clauses)
                if modalities:
                    by_source_params.extend([f"%{m}%" for m in modalities])
                    by_source_clauses.extend([sql.SQL("modality ILIKE %s")] * len(modalities))
                if search_pattern:
                    by_source_clauses.append(sql.SQL("(title ILIKE %s OR description ILIKE %s OR authors::text ILIKE %s)"))
                    by_source_params.extend([search_pattern, search_pattern, search_pattern])
                by_source_where = sql.SQL("")
                if by_source_clauses:
                    by_source_where = sql.SQL("WHERE ") + sql.SQL(" AND ").join(by_source_clauses)

                query_by_source = sql.SQL("""
                    SELECT source, COUNT(*) as count
                    FROM {table}
                    {where}
                    GROUP BY source
                    ORDER BY count DESC
                """).format(table=table_identifier, where=by_source_where)
                cursor.execute(query_by_source, by_source_params)
                by_source = {row["source"]: row["count"] for row in cursor.fetchall()}

                by_modality_params = []
                by_modality_clauses = list(visible_clauses)
                if source:
                    by_modality_clauses.append(sql.SQL("source = %s"))
                    by_modality_params.append(source)
                if search_pattern:
                    by_modality_clauses.append(sql.SQL("(title ILIKE %s OR description ILIKE %s OR authors::text ILIKE %s)"))
                    by_modality_params.extend([search_pattern, search_pattern, search_pattern])
                if modalities:
                    for m in modalities:
                        by_modality_clauses.append(sql.SQL("modality ILIKE %s"))
                        by_modality_params.append(f"%{m}%")

                by_modality_where = sql.SQL("")
                if by_modality_clauses:
                    by_modality_where = sql.SQL("WHERE ") + sql.SQL(" AND ").join(by_modality_clauses)

                # Dynamic modality facets:
                # - Split comma-separated modality strings into tokens
                # - Count occurrences across datasets that match current filters (source + selected modalities)
                query_by_modality = sql.SQL("""
                    SELECT
                        CASE
                            -- Preserve acronyms / tokens containing 2+ consecutive uppercase letters (e.g. EEG, fMRI, iEEG)
                            -- NOTE: avoid curly-brace quantifiers here because psycopg.sql uses braces for formatting.
                            WHEN token_raw ~ '.*[A-Z][A-Z]+.*' THEN token_raw
                            ELSE LOWER(token_raw)
                        END AS modality,
                        COUNT(*)::int AS count
                    FROM (
                        SELECT
                            TRIM(regexp_split_to_table(COALESCE(modality, ''), '\\s*[,;]\\s*')) AS token_raw
                        FROM {table}
                        {where}
                    ) t
                    WHERE token_raw <> ''
                    GROUP BY 1
                    ORDER BY count DESC, modality ASC
                    LIMIT 300
                """).format(table=table_identifier, where=by_modality_where)

                cursor.execute(query_by_modality, by_modality_params)
                by_modality = {row["modality"]: row["count"] for row in cursor.fetchall()}

                # Total count (apply BOTH filters)
                total_where_clauses = list(visible_clauses)
                total_params = []
                if source:
                    total_where_clauses.append(sql.SQL("source = %s"))
                    total_params.append(source)
                if search_pattern:
                    total_where_clauses.append(sql.SQL("(title ILIKE %s OR description ILIKE %s OR authors::text ILIKE %s)"))
                    total_params.extend([search_pattern, search_pattern, search_pattern])
                if modalities:
                    for m in modalities:
                        total_where_clauses.append(sql.SQL("modality ILIKE %s"))
                        total_params.append(f"%{m}%")

                total_where = sql.SQL("")
                if total_where_clauses:
                    total_where = sql.SQL("WHERE ") + sql.SQL(" AND ").join(total_where_clauses)

                query_total = sql.SQL("SELECT COUNT(*) as total FROM {table} {where}").format(
                    table=table_identifier,
                    where=total_where,
                )
                cursor.execute(query_total, total_params)
                total = cursor.fetchone()["total"]

                return {
                    "total": total,
                    "by_source": by_source,
                    "by_modality": by_modality
                }

    except psycopg.Error as e:
        logger.exception("Database query error in /api/datasets/stats")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.exception("Unexpected error in /api/datasets/stats")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# --- Dataset reuse metrics ---------------------------------------------------

# Paper-mapping tables per archive: the table-name prefix ({prefix}_dataset,
# {prefix}_paper_map, {prefix}_paper_citations,
# {prefix}_paper_citation_classifications) and their dataset id column.
_ARCHIVE_PAPER_TABLES: Dict[str, Tuple[str, str]] = {
    "DANDI": ("dandi", "dandi_id"),
    "OpenNeuro": ("openneuro", "openneuro_id"),
    "CRCNS": ("crcns", "crcns_id"),
    "SPARC": ("sparc", "sparc_id"),
}

# A citing paper is labelled once per primary paper it cites. Its label for the
# dataset is the first of these it has (the order the citation lists use).
LABEL_PRECEDENCE: Tuple[str, ...] = ("REUSE", "PRIMARY", "MENTION", "NEITHER")

# papers.text_status values meaning the mapping found no full text to classify.
NO_TEXT_STATUSES: Tuple[str, ...] = ("metadata_only", "unavailable")

_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}


def _author_key(name: Any) -> Optional[Tuple[str, str]]:
    """
    Loose identity for an author name: (last word of the surname, first initial).

    Reads both "Last, First M." (archive metadata) and "First M. Last"
    (OpenAlex). Accents, apostrophes, hyphens and stray invisible characters
    are dropped, so "Pirio-Richardson, Sarah" and "Sarah Pirio Richardson"
    match. None when the name lacks a surname or a first name.
    """
    if isinstance(name, dict):
        name = name.get("name")
    if not isinstance(name, str):
        return None
    text = unicodedata.normalize("NFKD", name)
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"['’]", "", text)
    if "," in text:
        last, _, first = text.partition(",")
        last_words = re.sub(r"[^a-z]+", " ", last).split()
        first_words = re.sub(r"[^a-z]+", " ", first).split()
    else:
        words = re.sub(r"[^a-z]+", " ", text).split()
        while len(words) > 2 and words[-1] in _NAME_SUFFIXES:
            words.pop()
        last_words, first_words = words[-1:], words[:-1]
    if not last_words or not first_words:
        return None
    return last_words[-1], first_words[0][0]


def _author_keys(names: Any) -> set:
    if not isinstance(names, list):
        return set()
    return {key for key in (_author_key(n) for n in names) if key}


def _first_author(authors: Any) -> Optional[str]:
    if not isinstance(authors, list) or not authors:
        return None
    first = authors[0]
    if isinstance(first, dict):
        first = first.get("name")
    return first if isinstance(first, str) else None


def _year_of(publication_year: Any, publication_date: Any) -> Optional[int]:
    if isinstance(publication_year, int):
        return publication_year
    if isinstance(publication_date, str) and re.match(r"\d{4}", publication_date):
        return int(publication_date[:4])
    return None


def _author_id_set(values: Any) -> set:
    return {v for v in values if isinstance(v, str) and v} if isinstance(values, list) else set()


def build_reuse_metrics(
    edges: List[Dict[str, Any]],
    *,
    dataset_authors: List[Any],
    primary_papers: List[Dict[str, Any]],
    published_year: Optional[int],
    current_year: int,
) -> Dict[str, Any]:
    """
    Reuse metrics for one dataset from its citation edges.

    `edges` has one row per (primary paper, citing paper): the edge's
    classification, status and same_lab, and the citing paper's work_key (see
    work_key_sql), title, publication date and year, text_status and (on REUSE
    rows) authors and OpenAlex author_ids. `dataset_authors` are the names the
    archive lists; `primary_papers` are the dataset's papers' `authors` and
    `author_ids`.

    Counts are of citing works: the versions of a paper (a preprint and its
    published version) count once, under the strongest label any version has,
    dated by the earliest version and shown as the published one.

    A reuse counts as same lab when the classifier said so; or it shares an
    OpenAlex author id with the primary papers (when both sides have ids); or
    an author's name matches the archive's author list (which has no ids), or
    the primary papers' authors when ids are missing. Otherwise it is
    independent. Years run from the dataset's publication (or the first dated
    paper, if earlier) to the current year.
    """
    papers: Dict[str, Dict[str, Any]] = {}
    for row in edges:
        paper = papers.setdefault(
            row["citing_paper_doi"], {"labels": set(), "statuses": set(), "same_lab": set()}
        )
        if row.get("classification"):
            paper["labels"].add(row["classification"])
        if row.get("status"):
            paper["statuses"].add(row["status"])
        if row.get("classification") == "REUSE" and row.get("same_lab") is not None:
            paper["same_lab"].add(bool(row["same_lab"]))
        for key in ("work_key", "publication_date", "publication_year", "text_status", "title", "authors", "author_ids"):
            if row.get(key) is not None:
                paper.setdefault(key, row[key])

    works: Dict[str, List[str]] = {}
    for doi, paper in papers.items():
        works.setdefault(paper.get("work_key") or f"d:{doi.lower()}", []).append(doi)

    def date_of(doi: str) -> Optional[str]:
        paper = papers[doi]
        year = paper.get("publication_year")
        return paper.get("publication_date") or (str(year) if year else None)

    def oldest_first(dois: List[str]) -> List[str]:
        return sorted(dois, key=lambda d: (date_of(d) is None, date_of(d) or "", d))

    dataset_keys = _author_keys(dataset_authors)
    primary_keys = set().union(*(_author_keys(p.get("authors")) for p in primary_papers))
    primary_ids = set().union(*(_author_id_set(p.get("author_ids")) for p in primary_papers))
    coverage = {"citing_papers": len(works), "classified": 0, "no_full_text": 0, "pending": 0}
    by_year: Dict[int, Dict[str, int]] = {}
    undated = {"reuse": 0, "mentions": 0}
    reuse_papers: List[Dict[str, Any]] = []
    mention_count = 0
    for dois in works.values():
        versions = [papers[d] for d in dois]
        labels = set().union(*(v["labels"] for v in versions))
        label = next((l for l in LABEL_PRECEDENCE if l in labels), None)
        if label:
            coverage["classified"] += 1
        elif all("no_full_text" in v["statuses"] or v.get("text_status") in NO_TEXT_STATUSES for v in versions):
            coverage["no_full_text"] += 1
        else:
            coverage["pending"] += 1
        if label not in ("REUSE", "MENTION"):
            continue

        bucket = "reuse" if label == "REUSE" else "mentions"
        first = oldest_first(dois)[0]
        year = _year_of(papers[first].get("publication_year"), papers[first].get("publication_date"))
        if year is None:
            undated[bucket] += 1
        else:
            by_year.setdefault(year, {"reuse": 0, "mentions": 0})[bucket] += 1
        if label == "MENTION":
            mention_count += 1
            continue

        shown = pick_published_version(dois)
        authors = next(
            (papers[d]["authors"] for d in [shown, *dois] if isinstance(papers[d].get("authors"), list)), []
        )
        citing_ids = set().union(*(_author_id_set(v.get("author_ids")) for v in versions))
        citing_keys = set().union(*(_author_keys(v.get("authors")) for v in versions))
        ids_known = bool(primary_ids and citing_ids)
        basis = []
        if any(True in v["same_lab"] for v in versions):
            basis.append("classifier")
        if ids_known and citing_ids & primary_ids:
            basis.append("author_ids")
        if citing_keys & dataset_keys or (not ids_known and citing_keys & primary_keys):
            basis.append("author_names")
        reuse_papers.append({
            "doi": shown,
            "title": papers[shown].get("title") or next((v["title"] for v in versions if v.get("title")), None),
            "first_author": _first_author(authors),
            "author_count": len(authors),
            "publication_date": papers[shown].get("publication_date"),
            "first_date": date_of(first),
            "versions": [
                {"doi": d, "is_preprint": is_preprint_doi(d), "publication_date": date_of(d)}
                for d in oldest_first(dois)
            ] if len(dois) > 1 else [],
            "same_lab": bool(basis),
            "same_lab_basis": basis,
        })

    reuse_papers.sort(key=lambda p: (p["first_date"] or "", p["doi"]), reverse=True)
    same_lab_count = sum(1 for p in reuse_papers if p["same_lab"])
    known_years = [y for y in (published_year, min(by_year, default=None)) if y is not None]
    per_year = []
    if known_years:
        start, end = min(known_years), max([current_year, *by_year])
        per_year = [
            {"year": y, **by_year.get(y, {"reuse": 0, "mentions": 0})} for y in range(start, end + 1)
        ]
    return {
        "reuse_count": len(reuse_papers),
        "independent_reuse_count": len(reuse_papers) - same_lab_count,
        "same_lab_reuse_count": same_lab_count,
        "mention_count": mention_count,
        "last_reuse": reuse_papers[0] if reuse_papers else None,
        "per_year": per_year,
        "undated": undated,
        "coverage": coverage,
        "reuse_papers": reuse_papers,
    }


def _fetch_metric_edges(cursor, prefix: str, id_col: str, dataset_id: str) -> List[Dict[str, Any]]:
    """The dataset's citation edges in the shape build_reuse_metrics reads."""
    citations = f"{prefix}_paper_citations"
    classifications = f"{prefix}_paper_citation_classifications"
    if not (_paper_mapping_relation_exists(cursor, citations) and _paper_mapping_relation_exists(cursor, "papers")):
        return []
    has_labels = _paper_mapping_relation_exists(cursor, classifications)
    if has_labels:
        label_cols = sql.SQL("cc.classification, cc.status, cc.same_lab")
        label_join = sql.SQL(
            "LEFT JOIN {cls} cc ON cc.{id} = c.{id} "
            "AND cc.primary_paper_doi = c.primary_paper_doi "
            "AND cc.citing_paper_doi = c.citing_paper_doi"
        ).format(cls=sql.Identifier(classifications), id=sql.Identifier(id_col))
        is_reuse = sql.SQL("cc.classification = 'REUSE'")
    else:
        label_cols = sql.SQL("NULL::text AS classification, NULL::text AS status, NULL::boolean AS same_lab")
        label_join = sql.SQL("")
        is_reuse = sql.SQL("FALSE")
    paper_columns = _table_columns(cursor, "papers")
    text_status = sql.SQL("p.text_status" if "text_status" in paper_columns else "NULL::text")
    author_ids = sql.SQL("p.author_ids" if "author_ids" in paper_columns else "NULL::jsonb")
    query = sql.SQL(
        """
        SELECT
            c.citing_paper_doi,
            {label_cols},
            {work_key} AS work_key,
            p.title,
            p.publication_date,
            p.publication_year,
            {text_status} AS text_status,
            CASE WHEN {is_reuse} THEN p.authors END AS authors,
            CASE WHEN {is_reuse} THEN {author_ids} END AS author_ids
        FROM {citations} c
        {label_join}
        LEFT JOIN papers p ON p.paper_doi = c.citing_paper_doi
        WHERE c.{id} = %s;
        """
    ).format(
        label_cols=label_cols,
        work_key=sql.SQL(work_key_sql("p.title", "c.citing_paper_doi")),
        text_status=text_status,
        author_ids=author_ids,
        is_reuse=is_reuse,
        citations=sql.Identifier(citations),
        label_join=label_join,
        id=sql.Identifier(id_col),
    )
    cursor.execute(query, (dataset_id,))
    return _clean_paper_titles([dict(r) for r in cursor.fetchall()])


# Registered before the detail route: its `{dataset_id:path}` would otherwise
# read "<id>/metrics" as the dataset id.
@app.get("/api/datasets/{source}/{dataset_id:path}/metrics")
async def get_dataset_metrics(source: str, dataset_id: str):
    """
    Reuse metrics for one dataset: reuse, independent reuse, mentions, counts
    per year, the latest reuse and how many citing papers were classified
    (see build_reuse_metrics). Archives without paper mapping answer
    {"tracked": false}.
    """
    canonical_source = {s.lower(): s for s in ALLOWED_SOURCES}.get(source.lower())
    if canonical_source is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    base = {"source": canonical_source, "dataset_id": dataset_id}
    if canonical_source not in _ARCHIVE_PAPER_TABLES:
        return {**base, "tracked": False}
    prefix, id_col = _ARCHIVE_PAPER_TABLES[canonical_source]
    dataset_table, map_table = f"{prefix}_dataset", f"{prefix}_paper_map"
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                if not _paper_mapping_relation_exists(cursor, dataset_table):
                    raise HTTPException(status_code=404, detail="Dataset not found")
                columns = _table_columns(cursor, dataset_table)
                authors_col = sql.SQL("authors" if "authors" in columns else "NULL::jsonb AS authors")
                precision_col = sql.SQL(
                    "created_at_precision" if "created_at_precision" in columns else "NULL::text AS created_at_precision"
                )
                cursor.execute(
                    sql.SQL("SELECT created_at, {precision}, {authors} FROM {table} WHERE dataset_id = %s LIMIT 1;").format(
                        precision=precision_col, authors=authors_col, table=sql.Identifier(dataset_table)
                    ),
                    (dataset_id,),
                )
                dataset = cursor.fetchone()
                if not dataset:
                    raise HTTPException(status_code=404, detail="Dataset not found")

                dataset_authors: List[Any] = list(dataset["authors"]) if isinstance(dataset["authors"], list) else []
                primary_papers: List[Dict[str, Any]] = []
                if _paper_mapping_relation_exists(cursor, map_table) and _paper_mapping_relation_exists(cursor, "papers"):
                    author_ids_col = sql.SQL(
                        "p.author_ids" if "author_ids" in _table_columns(cursor, "papers") else "NULL::jsonb AS author_ids"
                    )
                    cursor.execute(
                        sql.SQL(
                            "SELECT p.authors, {author_ids} FROM {map} m JOIN papers p ON p.paper_doi = m.paper_doi "
                            "WHERE m.{id} = %s;"
                        ).format(author_ids=author_ids_col, map=sql.Identifier(map_table), id=sql.Identifier(id_col)),
                        (dataset_id,),
                    )
                    primary_papers = [dict(row) for row in cursor.fetchall()]
                edges = _fetch_metric_edges(cursor, prefix, id_col, dataset_id)
    except HTTPException:
        raise
    except psycopg.Error as e:
        logger.exception("Database query error in /api/datasets/%s/%s/metrics", canonical_source, dataset_id)
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

    created_at = dataset["created_at"]
    metrics = build_reuse_metrics(
        edges,
        dataset_authors=dataset_authors,
        primary_papers=primary_papers,
        published_year=created_at.year if created_at else None,
        current_year=datetime.now(timezone.utc).year,
    )
    return {
        **base,
        "tracked": True,
        "published": created_at.date().isoformat() if created_at else None,
        # "year" when only the publication year is known (CRCNS); null means a real date.
        "published_precision": dataset["created_at_precision"],
        **metrics,
    }


@app.get("/api/datasets/{source}/{dataset_id:path}")
async def get_dataset_detail(source: str, dataset_id: str):
    """
    Fetch a single dataset by its source and archive-native ID (e.g. crcns/590,
    dandi/000003, openneuro/ds000001), including associated primary papers and citing
    papers when available. The source is matched case-insensitively; the (source,
    dataset_id) pair is unique, so this resolves the dataset unambiguously.
    """
    # Normalize the URL source (lowercase) to the canonical stored form.
    canonical_source = {s.lower(): s for s in ALLOWED_SOURCES}.get(source.lower())
    if canonical_source is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                # --- resolve dataset from unified_datasets (or fallback) ---
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.views
                        WHERE table_schema = 'public' AND table_name = 'unified_datasets'
                    );
                    """,
                )
                view_exists = cursor.fetchone()["exists"]
                table_name = "unified_datasets" if view_exists else "neuroscience_datasets"

                # Build column list dynamically so missing columns don't break the query
                base_cols = ["source", "dataset_id", "title", "modality", "papers", "url",
                             "description", "created_at", "updated_at"]
                optional_cols = ["full_description", "authors", "contributors", "license", "num_subjects",
                                 "created_at_precision"]
                cursor.execute(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name = %s;""",
                    (table_name,),
                )
                existing_cols = {row["column_name"] for row in cursor.fetchall()}
                select_cols = base_cols + [c for c in optional_cols if c in existing_cols]

                cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in select_cols)
                detail_query = sql.SQL(
                    "SELECT {cols} FROM {table} WHERE source = %s AND dataset_id = %s LIMIT 1;"
                ).format(cols=cols_sql, table=sql.Identifier(table_name))
                cursor.execute(detail_query, (canonical_source, dataset_id))
                dataset = cursor.fetchone()
                if not dataset:
                    raise HTTPException(status_code=404, detail="Dataset not found")

                dataset = dict(dataset)
                source = dataset["source"]

                # --- primary papers (best-effort; paper mapping tables may not exist) ---
                primary_papers: list[dict] = []
                citations: list[dict] = []
                try:
                    _ensure_paper_mapping_tables(cursor)

                    # Check which optional paper columns exist
                    cursor.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'papers'
                          AND column_name IN ('journal', 'senior_author_country', 'text_status')
                    """)
                    paper_opt_cols = {r["column_name"] for r in cursor.fetchall()}
                    p_journal = "p.journal," if "journal" in paper_opt_cols else "NULL AS journal,"
                    p_country = "p.senior_author_country," if "senior_author_country" in paper_opt_cols else "NULL AS senior_author_country,"
                    c_text_status = "p_citing.text_status AS citing_text_status," if "text_status" in paper_opt_cols else "NULL::text AS citing_text_status,"

                    primary_papers_query = f"""
                        {_paper_mapping_ctes(cursor)}
                        SELECT
                            map.paper_doi,
                            map.doi_source,
                            map.relation_type,
                            p.title AS paper_title,
                            p.authors,
                            p.openalex_id,
                            {p_journal}
                            {p_country}
                            p.publication_date,
                            p.publication_year,
                            {work_key_sql('p.title', 'map.paper_doi')} AS work_key,
                            COUNT(DISTINCT ce.citing_paper_doi)::int AS citing_papers_count
                        FROM dataset_map map
                        LEFT JOIN papers p ON p.paper_doi = map.paper_doi
                        LEFT JOIN citation_edges ce
                          ON ce.source = map.source
                         AND ce.dataset_id = map.dataset_id
                         AND ce.primary_paper_doi = map.paper_doi
                        WHERE map.source = %s AND map.dataset_id = %s
                        GROUP BY
                            map.paper_doi, map.doi_source, map.relation_type,
                            p.title, p.authors, p.openalex_id,
                            {('p.journal,' if 'journal' in paper_opt_cols else '')}
                            {('p.senior_author_country,' if 'senior_author_country' in paper_opt_cols else '')}
                            p.publication_date, p.publication_year
                        ORDER BY COALESCE(p.publication_date, '') DESC, map.paper_doi ASC;
                    """
                    cursor.execute(primary_papers_query, [source, dataset_id])
                    primary_papers = _annotate_versions(
                        _clean_paper_titles([dict(r) for r in cursor.fetchall()]),
                        doi_key="paper_doi", work_key="work_key", date_key="publication_date",
                    )

                    # What the site shows per primary paper: the works citing any
                    # version of it, so a paper citing both a preprint and its
                    # published version counts once. Counted over every edge,
                    # not just the citations returned below.
                    citing_works_query = f"""
                        {_paper_mapping_ctes(cursor)}
                        SELECT
                            {work_key_sql('p.title', 'map.paper_doi')} AS work_key,
                            COUNT(DISTINCT {work_key_sql('p_citing.title', 'ce.citing_paper_doi')})::int
                                AS citing_works_count
                        FROM dataset_map map
                        LEFT JOIN papers p ON p.paper_doi = map.paper_doi
                        JOIN citation_edges ce
                          ON ce.source = map.source
                         AND ce.dataset_id = map.dataset_id
                         AND ce.primary_paper_doi = map.paper_doi
                        LEFT JOIN papers p_citing ON p_citing.paper_doi = ce.citing_paper_doi
                        WHERE map.source = %s AND map.dataset_id = %s
                        GROUP BY 1;
                    """
                    cursor.execute(citing_works_query, [source, dataset_id])
                    citing_works = {r["work_key"]: r["citing_works_count"] for r in cursor.fetchall()}
                    for paper in primary_papers:
                        paper["citing_works_count"] = citing_works.get(paper.get("work_key"), 0)

                    c_journal ="p_citing.journal AS citing_journal," if "journal" in paper_opt_cols else "NULL AS citing_journal,"
                    c_country = "p_citing.senior_author_country AS citing_senior_author_country," if "senior_author_country" in paper_opt_cols else "NULL AS citing_senior_author_country,"

                    citations_query = f"""
                        {_paper_mapping_ctes(cursor)}
                        SELECT
                            ce.primary_paper_doi,
                            p_primary.title AS primary_paper_title,
                            ce.citing_paper_doi,
                            p_citing.title AS citing_paper_title,
                            {work_key_sql('p_citing.title', 'ce.citing_paper_doi')} AS citing_work_key,
                            p_citing.authors AS citing_authors,
                            {c_journal}
                            {c_country}
                            p_citing.publication_date AS citing_publication_date,
                            p_citing.publication_year AS citing_publication_year,
                            {c_text_status}
                            COALESCE(NULLIF(cc.classification, ''), cc.status, 'unclassified') AS classification_status,
                            cc.classification,
                            cc.confidence,
                            cc.reasoning,
                            cc.status,
                            cc.mode,
                            cc.prompt_version,
                            cc.classification_model,
                            cc.reuse_type,
                            cc.reuse_type_other,
                            cc.reused_modalities,
                            cc.reused_dandi_hosted,
                            cc.same_lab,
                            cc.same_lab_confidence,
                            cc.source_archive,
                            cc.evidence_quotes,
                            cc.hallucinated_quote_count
                        FROM citation_edges ce
                        LEFT JOIN papers p_primary ON p_primary.paper_doi = ce.primary_paper_doi
                        LEFT JOIN papers p_citing ON p_citing.paper_doi = ce.citing_paper_doi
                        LEFT JOIN citation_classifications cc
                          ON cc.source = ce.source
                         AND cc.dataset_id = ce.dataset_id
                         AND cc.primary_paper_doi = ce.primary_paper_doi
                         AND cc.citing_paper_doi = ce.citing_paper_doi
                        WHERE ce.source = %s AND ce.dataset_id = %s
                        ORDER BY {CITATION_DISPLAY_ORDER_SQL},
                                 COALESCE(p_citing.publication_date, '') DESC,
                                 ce.citing_paper_doi ASC
                        LIMIT 250;
                    """
                    cursor.execute(citations_query, [source, dataset_id])
                    citations = _annotate_versions(
                        _clean_paper_titles([dict(r) for r in cursor.fetchall()]),
                        doi_key="citing_paper_doi", work_key="citing_work_key",
                        date_key="citing_publication_date", prefix="citing_",
                    )

                except HTTPException:
                    pass

                return {
                    "dataset": dataset,
                    "primary_papers": primary_papers,
                    "citations": citations,
                }
    except HTTPException:
        raise
    except psycopg.Error as e:
        logger.exception("Database query error in /api/datasets/%s", dataset_id)
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.exception("Unexpected error in /api/datasets/%s", dataset_id)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


_DATASET_FUNNEL_KEYS = (
    "ingested_total", "junk_datasets", "ingested_datasets",
    "no_paper_datasets", "pending_datasets", "never_published_datasets", "junk_reasons",
)


def _dataset_funnel(cursor, source: str) -> Dict[str, Any]:
    """
    Where an archive's ingested datasets stand, from {prefix}_dataset.dataset_status
    (written by utils/dataset_status.py in the ingestion and mapping DAGs):

      ingested_total       every row ingestion wrote
      junk_datasets        dataset_status = 'excluded' (test / placeholder / empty uploads)
      ingested_datasets    ingested_total - junk_datasets: the count the site shows
      no_paper_datasets    mapping ran, the archive's metadata named no paper
      pending_datasets     not mapped yet (NULL status counts as pending)
      never_published_datasets  DANDI only: draft-only dandisets that are not junk
      junk_reasons         {reason: count} behind junk_datasets

    All None when the table or the column does not exist yet (older schema).
    """
    empty = {k: None for k in _DATASET_FUNNEL_KEYS}
    tables = _ARCHIVE_PAPER_TABLES.get(source)
    if not tables:
        return empty
    ds_tbl = f"{tables[0]}_dataset"
    if not _paper_mapping_relation_exists(cursor, ds_tbl):
        return empty
    if not _relation_has_column(cursor, ds_tbl, "dataset_status"):
        return empty
    never_published_sql = "NULL::int"
    if source == "DANDI" and _relation_has_column(cursor, ds_tbl, "version"):
        never_published_sql = (
            "COUNT(*) FILTER (WHERE version = 'draft' "
            "AND COALESCE(dataset_status, '') <> 'excluded')::int"
        )
    cursor.execute(f"""
        SELECT
            COUNT(*)::int AS ingested_total,
            COUNT(*) FILTER (WHERE dataset_status = 'excluded')::int AS junk_datasets,
            COUNT(*) FILTER (WHERE COALESCE(dataset_status, '') <> 'excluded')::int AS ingested_datasets,
            COUNT(*) FILTER (WHERE dataset_status = 'no_paper')::int AS no_paper_datasets,
            COUNT(*) FILTER (WHERE dataset_status = 'pending' OR dataset_status IS NULL)::int AS pending_datasets,
            {never_published_sql} AS never_published_datasets
        FROM {ds_tbl};
    """)
    row = dict(cursor.fetchone() or {})
    cursor.execute(f"""
        SELECT COALESCE(dataset_status_reason, 'unknown') AS reason, COUNT(*)::int AS n
        FROM {ds_tbl}
        WHERE dataset_status = 'excluded'
        GROUP BY 1
        ORDER BY n DESC, reason ASC;
    """)
    row["junk_reasons"] = {r["reason"]: r["n"] for r in cursor.fetchall()}
    return {k: row.get(k) for k in _DATASET_FUNNEL_KEYS}


@app.get("/api/paper-mapping/summary")
async def get_paper_mapping_summary(
    source: Optional[str] = Query(None, description="Filter by source (CRCNS, DANDI, OpenNeuro, SPARC)"),
):
    source = _validate_paper_mapping_source(source)
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                _ensure_paper_mapping_tables(cursor)

                # Query underlying tables directly — no CTEs, no UNION ALL
                # overhead.  Each SELECT hits one indexed table.
                def _per_source_summary(src_label: str, map_tbl: str, id_col: str,
                                        cit_tbl: str, cls_tbl: str) -> Dict[str, Any]:
                    cursor.execute(f"""
                        SELECT
                            COUNT(DISTINCT {id_col})::int AS datasets_with_mapped_papers,
                            COUNT(DISTINCT paper_doi)::int AS distinct_mapped_primary_papers
                        FROM {map_tbl};
                    """)
                    map_row = dict(cursor.fetchone() or {})
                    funnel_row = _dataset_funnel(cursor, src_label)

                    ctx_expr = (
                        f"CASE WHEN jsonb_typeof(citation_contexts) = 'array' "
                        f"THEN jsonb_array_length(citation_contexts) ELSE 0 END"
                    )
                    cursor.execute(f"""
                        SELECT
                            COUNT(*)::int AS citation_edges,
                            COUNT(CASE WHEN contexts_extracted_at IS NOT NULL THEN 1 END)::int AS citations_with_contexts,
                            COALESCE(SUM({ctx_expr}), 0)::int AS citation_context_count
                        FROM {cit_tbl};
                    """)
                    cit_row = dict(cursor.fetchone() or {})

                    cursor.execute(f"""
                        SELECT
                            COUNT(CASE WHEN classification IS NOT NULL THEN 1 END)::int AS classified_edges,
                            COUNT(CASE WHEN status = 'placeholder' THEN 1 END)::int AS placeholder_classification_edges
                        FROM {cls_tbl};
                    """)
                    cls_row = dict(cursor.fetchone() or {})
                    return {
                        "source": src_label,
                        **map_row, **cit_row, **cls_row, **funnel_row,
                    }

                # CRCNS and SPARC paper-mapping tables are optional (their
                # mapping DAGs may not have run on every deployment). Include
                # each only when its three tables exist, mirroring
                # _paper_mapping_ctes.
                crcns_tables_exist = (
                    _paper_mapping_relation_exists(cursor, "crcns_paper_map")
                    and _paper_mapping_relation_exists(cursor, "crcns_paper_citations")
                    and _paper_mapping_relation_exists(cursor, "crcns_paper_citation_classifications")
                )
                sparc_tables_exist = (
                    _paper_mapping_relation_exists(cursor, "sparc_paper_map")
                    and _paper_mapping_relation_exists(cursor, "sparc_paper_citations")
                    and _paper_mapping_relation_exists(cursor, "sparc_paper_citation_classifications")
                )

                source_configs = []
                if not source or source == "DANDI":
                    source_configs.append(("DANDI", "dandi_paper_map", "dandi_id",
                                           "dandi_paper_citations", "dandi_paper_citation_classifications"))
                if not source or source == "OpenNeuro":
                    source_configs.append(("OpenNeuro", "openneuro_paper_map", "openneuro_id",
                                           "openneuro_paper_citations", "openneuro_paper_citation_classifications"))
                if (not source or source == "CRCNS") and crcns_tables_exist:
                    source_configs.append(("CRCNS", "crcns_paper_map", "crcns_id",
                                           "crcns_paper_citations", "crcns_paper_citation_classifications"))
                if (not source or source == "SPARC") and sparc_tables_exist:
                    source_configs.append(("SPARC", "sparc_paper_map", "sparc_id",
                                           "sparc_paper_citations", "sparc_paper_citation_classifications"))

                by_source = [_per_source_summary(*cfg) for cfg in source_configs]

                # Build overall summary by summing per-source values.
                # distinct_mapped_primary_papers needs dedup across sources
                # (a DOI could appear in multiple maps).
                all_dois_parts = []
                if not source or source == "DANDI":
                    all_dois_parts.append("SELECT DISTINCT paper_doi FROM dandi_paper_map")
                if not source or source == "OpenNeuro":
                    all_dois_parts.append("SELECT DISTINCT paper_doi FROM openneuro_paper_map")
                if (not source or source == "CRCNS") and crcns_tables_exist:
                    all_dois_parts.append("SELECT DISTINCT paper_doi FROM crcns_paper_map")
                if (not source or source == "SPARC") and sparc_tables_exist:
                    all_dois_parts.append("SELECT DISTINCT paper_doi FROM sparc_paper_map")
                if all_dois_parts:
                    cursor.execute(f"SELECT COUNT(DISTINCT paper_doi)::int AS n FROM ({' UNION ALL '.join(all_dois_parts)}) t;")
                    distinct_papers = (cursor.fetchone() or {}).get("n", 0)
                else:
                    distinct_papers = 0

                def _sum_or_none(key: str) -> Optional[int]:
                    vals = [r.get(key) for r in by_source]
                    known = [v for v in vals if isinstance(v, int)]
                    return sum(known) if known else None

                summary = {
                    # Dataset funnel (utils/dataset_status.py): ingested_total is every
                    # row the ingestion DAGs wrote; junk is excluded from ingested_datasets,
                    # which is what the site shows as the archive's dataset count.
                    "ingested_total": _sum_or_none("ingested_total"),
                    "junk_datasets": _sum_or_none("junk_datasets"),
                    "ingested_datasets": _sum_or_none("ingested_datasets"),
                    "no_paper_datasets": _sum_or_none("no_paper_datasets"),
                    "pending_datasets": _sum_or_none("pending_datasets"),
                    "datasets_with_mapped_papers": sum(r.get("datasets_with_mapped_papers", 0) for r in by_source),
                    "distinct_mapped_primary_papers": distinct_papers,
                    "citation_edges": sum(r.get("citation_edges", 0) for r in by_source),
                    "citations_with_contexts": sum(r.get("citations_with_contexts", 0) for r in by_source),
                    "citation_context_count": sum(r.get("citation_context_count", 0) for r in by_source),
                    "classified_edges": sum(r.get("classified_edges", 0) for r in by_source),
                    "placeholder_classification_edges": sum(r.get("placeholder_classification_edges", 0) for r in by_source),
                }

                # Classification bucket breakdown
                cls_parts = []
                if not source or source == "DANDI":
                    cls_parts.append("""
                        SELECT COALESCE(NULLIF(classification, ''), status, 'unclassified') AS bucket
                        FROM dandi_paper_citation_classifications
                    """)
                if not source or source == "OpenNeuro":
                    cls_parts.append("""
                        SELECT COALESCE(NULLIF(classification, ''), status, 'unclassified') AS bucket
                        FROM openneuro_paper_citation_classifications
                    """)
                if (not source or source == "CRCNS") and crcns_tables_exist:
                    cls_parts.append("""
                        SELECT COALESCE(NULLIF(classification, ''), status, 'unclassified') AS bucket
                        FROM crcns_paper_citation_classifications
                    """)
                if (not source or source == "SPARC") and sparc_tables_exist:
                    cls_parts.append("""
                        SELECT COALESCE(NULLIF(classification, ''), status, 'unclassified') AS bucket
                        FROM sparc_paper_citation_classifications
                    """)
                by_classification: Dict[str, int] = {}
                if cls_parts:
                    cursor.execute(f"""
                        SELECT bucket, COUNT(*)::int AS count
                        FROM ({' UNION ALL '.join(cls_parts)}) t
                        GROUP BY 1 ORDER BY count DESC, bucket ASC;
                    """)
                    by_classification = {row["bucket"]: row["count"] for row in cursor.fetchall()}

                return {
                    "summary": summary,
                    "by_source": by_source,
                    "by_classification": by_classification,
                }
    except HTTPException:
        raise
    except psycopg.Error as e:
        logger.exception("Database query error in /api/paper-mapping/summary")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.exception("Unexpected error in /api/paper-mapping/summary")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/paper-mapping/datasets")
async def get_paper_mapping_datasets(
    source: Optional[str] = Query(None, description="Filter by source (CRCNS, DANDI, OpenNeuro, SPARC)"),
    search: Optional[str] = Query(None, description="Search in dataset id, title, and description"),
    classification_bucket: Optional[str] = Query(
        None,
        description=(
            "Only datasets with at least one citation edge whose bucket matches "
            "COALESCE(NULLIF(classification,''), status, 'unclassified') — same keys as summary by_classification "
            "(e.g. REUSE, MENTION, NEITHER, PRIMARY, placeholder, error, no_full_text)."
        ),
    ),
    sort_by: str = Query(
        "mapped_papers",
        description="Sort by mapped_papers, citation_edges, contexts_extracted, latest_primary_publication_date, latest_citing_publication_date, title, id, source",
    ),
    sort_order: str = Query("desc", description="Sort order (asc, desc)"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    source = _validate_paper_mapping_source(source)
    sort_by_norm = (sort_by or "mapped_papers").strip().lower()
    sort_order_norm = (sort_order or "desc").strip().lower()
    if sort_order_norm not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail=f"Invalid sort_order: {sort_order}")
    sort_column_by_key = {
        "mapped_papers": "mapped_papers_count",
        "citation_edges": "citation_edges_count",
        "contexts_extracted": "contexts_extracted_count",
        "latest_primary_publication_date": "latest_primary_publication_date",
        "latest_citing_publication_date": "latest_citing_publication_date",
        "title": "dataset_title",
        "id": "dataset_id",
        "source": "source",
    }
    sort_col = sort_column_by_key.get(sort_by_norm)
    if not sort_col:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by: {sort_by}")

    bucket_sql = ""
    bucket_params: List[Any] = []
    if classification_bucket and classification_bucket.strip():
        bucket_sql = """
            WHERE EXISTS (
                SELECT 1 FROM citation_classifications cc
                WHERE cc.source = a.source
                  AND cc.dataset_id = a.dataset_id
                  AND COALESCE(NULLIF(cc.classification, ''), cc.status, 'unclassified') = %s
            )
        """
        bucket_params = [classification_bucket.strip()]

    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                _ensure_paper_mapping_tables(cursor)
                where_sql, params = _paper_mapping_filter_sql(source=source, search=search)

                # Pre-aggregate each dimension to one row per (source, dataset_id)
                # so the final join is 1:1 — avoids the cartesian product between
                # maps × citations × classifications that blows up at scale.
                aggregated_cte = f"""
                    {_paper_mapping_ctes(cursor)},
                    map_agg AS (
                        SELECT source, dataset_id,
                               COUNT(DISTINCT paper_doi)::int AS mapped_papers_count,
                               MAX(resolved_at::text) AS latest_resolved_at
                        FROM dataset_map
                        GROUP BY source, dataset_id
                    ),
                    map_pub AS (
                        SELECT m.source, m.dataset_id,
                               MAX(p.publication_date) AS latest_primary_publication_date
                        FROM dataset_map m
                        JOIN papers p ON p.paper_doi = m.paper_doi
                        GROUP BY m.source, m.dataset_id
                    ),
                    ce_agg AS (
                        SELECT source, dataset_id,
                               COUNT(*)::int AS citation_edges_count,
                               COUNT(CASE WHEN contexts_extracted_at IS NOT NULL THEN 1 END)::int AS citations_with_contexts_count,
                               COALESCE(SUM({_count_contexts_expr('citation_edges')}), 0)::int AS contexts_extracted_count,
                               MAX(citing_publication_date) AS latest_citing_publication_date
                        FROM citation_edges
                        GROUP BY source, dataset_id
                    ),
                    cc_agg AS (
                        SELECT source, dataset_id,
                               COUNT(CASE WHEN classification IS NOT NULL THEN 1 END)::int AS classified_edges_count,
                               COUNT(CASE WHEN status = 'placeholder' THEN 1 END)::int AS placeholder_classification_edges_count
                        FROM citation_classifications
                        GROUP BY source, dataset_id
                    ),
                    aggregated AS (
                        SELECT
                            db.source,
                            db.dataset_id,
                            db.dataset_title,
                            db.dataset_description,
                            db.modality,
                            db.papers_count,
                            db.created_at,
                            db.updated_at,
                            db.url,
                            COALESCE(ma.mapped_papers_count, 0) AS mapped_papers_count,
                            COALESCE(cea.citation_edges_count, 0) AS citation_edges_count,
                            COALESCE(cea.citations_with_contexts_count, 0) AS citations_with_contexts_count,
                            COALESCE(cea.contexts_extracted_count, 0) AS contexts_extracted_count,
                            mp.latest_primary_publication_date,
                            cea.latest_citing_publication_date,
                            COALESCE(cca.classified_edges_count, 0) AS classified_edges_count,
                            COALESCE(cca.placeholder_classification_edges_count, 0) AS placeholder_classification_edges_count
                        FROM dataset_base db
                        LEFT JOIN map_agg ma ON ma.source = db.source AND ma.dataset_id = db.dataset_id
                        LEFT JOIN map_pub mp ON mp.source = db.source AND mp.dataset_id = db.dataset_id
                        LEFT JOIN ce_agg cea ON cea.source = db.source AND cea.dataset_id = db.dataset_id
                        LEFT JOIN cc_agg cca ON cca.source = db.source AND cca.dataset_id = db.dataset_id
                        {where_sql}
                    )
                """
                count_query = f"{aggregated_cte} SELECT COUNT(*)::int AS total FROM aggregated a{bucket_sql};"
                cursor.execute(count_query, params + bucket_params)
                total = cursor.fetchone()["total"]

                query = f"""
                    {aggregated_cte}
                    SELECT *
                    FROM aggregated a{bucket_sql}
                    ORDER BY {sort_col} {sort_order_norm.upper()} NULLS LAST, dataset_title ASC, dataset_id ASC
                    LIMIT %s OFFSET %s;
                """
                cursor.execute(query, params + bucket_params + [limit, offset])
                rows = [dict(row) for row in cursor.fetchall()]
                return {"datasets": rows, "count": total}
    except HTTPException:
        raise
    except psycopg.Error as e:
        logger.exception("Database query error in /api/paper-mapping/datasets")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.exception("Unexpected error in /api/paper-mapping/datasets")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/paper-mapping/datasets/{source}/{dataset_id}")
async def get_paper_mapping_dataset_detail(source: str, dataset_id: str):
    source = _validate_paper_mapping_source(source)
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                _ensure_paper_mapping_tables(cursor)

                base_query = f"""
                    {_paper_mapping_ctes(cursor)}
                    SELECT
                        db.source,
                        db.dataset_id,
                        db.dataset_title,
                        db.dataset_description,
                        db.modality,
                        db.papers_count,
                        db.created_at,
                        db.updated_at,
                        db.url,
                        COALESCE(ma.mapped_papers_count, 0) AS mapped_papers_count,
                        COALESCE(cea.citation_edges_count, 0) AS citation_edges_count,
                        COALESCE(cea.citations_with_contexts_count, 0) AS citations_with_contexts_count,
                        COALESCE(cea.contexts_extracted_count, 0) AS contexts_extracted_count
                    FROM dataset_base db
                    LEFT JOIN (
                        SELECT source, dataset_id, COUNT(DISTINCT paper_doi)::int AS mapped_papers_count
                        FROM dataset_map GROUP BY source, dataset_id
                    ) ma ON ma.source = db.source AND ma.dataset_id = db.dataset_id
                    LEFT JOIN (
                        SELECT source, dataset_id,
                               COUNT(*)::int AS citation_edges_count,
                               COUNT(CASE WHEN contexts_extracted_at IS NOT NULL THEN 1 END)::int AS citations_with_contexts_count,
                               COALESCE(SUM({_count_contexts_expr('citation_edges')}), 0)::int AS contexts_extracted_count
                        FROM citation_edges GROUP BY source, dataset_id
                    ) cea ON cea.source = db.source AND cea.dataset_id = db.dataset_id
                    WHERE db.source = %s AND db.dataset_id = %s;
                """
                cursor.execute(base_query, [source, dataset_id])
                dataset = cursor.fetchone()
                if not dataset:
                    raise HTTPException(status_code=404, detail="Dataset not found in paper mapping tables")

                primary_papers_query = f"""
                    {_paper_mapping_ctes(cursor)},
                    ce_per_primary AS (
                        SELECT source, dataset_id, primary_paper_doi,
                               COUNT(DISTINCT citing_paper_doi)::int AS citing_papers_count,
                               COUNT(DISTINCT CASE WHEN contexts_extracted_at IS NOT NULL THEN citing_paper_doi END)::int AS citations_with_contexts_count
                        FROM citation_edges
                        WHERE source = %s AND dataset_id = %s
                        GROUP BY source, dataset_id, primary_paper_doi
                    ),
                    cc_per_primary AS (
                        SELECT source, dataset_id, primary_paper_doi,
                               COUNT(CASE WHEN classification IS NOT NULL THEN 1 END)::int AS classified_edges_count,
                               COUNT(CASE WHEN status = 'placeholder' THEN 1 END)::int AS placeholder_classification_edges_count
                        FROM citation_classifications
                        WHERE source = %s AND dataset_id = %s
                        GROUP BY source, dataset_id, primary_paper_doi
                    )
                    SELECT
                        map.paper_doi,
                        map.doi_source,
                        map.relation_type,
                        map.resolved_at,
                        map.run_id,
                        p.title AS paper_title,
                        p.authors,
                        p.openalex_id,
                        p.publication_date,
                        p.publication_year,
                        COALESCE(cepp.citing_papers_count, 0) AS citing_papers_count,
                        COALESCE(cepp.citations_with_contexts_count, 0) AS citations_with_contexts_count,
                        COALESCE(ccpp.classified_edges_count, 0) AS classified_edges_count,
                        COALESCE(ccpp.placeholder_classification_edges_count, 0) AS placeholder_classification_edges_count
                    FROM dataset_map map
                    LEFT JOIN papers p
                      ON p.paper_doi = map.paper_doi
                    LEFT JOIN ce_per_primary cepp
                      ON cepp.source = map.source AND cepp.dataset_id = map.dataset_id AND cepp.primary_paper_doi = map.paper_doi
                    LEFT JOIN cc_per_primary ccpp
                      ON ccpp.source = map.source AND ccpp.dataset_id = map.dataset_id AND ccpp.primary_paper_doi = map.paper_doi
                    WHERE map.source = %s AND map.dataset_id = %s
                    ORDER BY COALESCE(p.publication_date, '') DESC, map.paper_doi ASC;
                """
                cursor.execute(primary_papers_query, [source, dataset_id, source, dataset_id, source, dataset_id])
                primary_papers = _clean_paper_titles([dict(row) for row in cursor.fetchall()])

                c_text_status = (
                    "p_citing.text_status AS citing_text_status,"
                    if "text_status" in _table_columns(cursor, "papers")
                    else "NULL::text AS citing_text_status,"
                )
                citations_query = f"""
                    {_paper_mapping_ctes(cursor)}
                    SELECT
                        ce.primary_paper_doi,
                        p_primary.title AS primary_paper_title,
                        ce.citing_paper_doi,
                        p_citing.title AS citing_paper_title,
                        p_citing.authors AS citing_authors,
                        p_citing.publication_date AS citing_publication_date_from_papers,
                        p_citing.publication_year AS citing_publication_year,
                        {c_text_status}
                        ce.citing_publication_date,
                        ce.citation_source,
                        ce.matched_primary_paper_doi,
                        ce.matched_primary_openalex_id,
                        ce.citation_contexts,
                        ce.contexts_extracted_at,
                        COALESCE(NULLIF(cc.classification, ''), cc.status, 'unclassified') AS classification_status,
                        cc.classification,
                        cc.same_lab,
                        cc.same_lab_confidence,
                        cc.confidence,
                        cc.status,
                        cc.reasoning,
                        cc.classification_model,
                        cc.classified_at,
                        cc.mode,
                        cc.prompt_version,
                        cc.reuse_type,
                        cc.reuse_type_other,
                        cc.reused_modalities,
                        cc.reused_dandi_hosted,
                        cc.source_archive,
                        cc.evidence_quotes,
                        cc.hallucinated_quote_count,
                        cc.error_kind
                    FROM citation_edges ce
                    LEFT JOIN papers p_primary
                      ON p_primary.paper_doi = ce.primary_paper_doi
                    LEFT JOIN papers p_citing
                      ON p_citing.paper_doi = ce.citing_paper_doi
                    LEFT JOIN citation_classifications cc
                      ON cc.source = ce.source
                     AND cc.dataset_id = ce.dataset_id
                     AND cc.primary_paper_doi = ce.primary_paper_doi
                     AND cc.citing_paper_doi = ce.citing_paper_doi
                    WHERE ce.source = %s AND ce.dataset_id = %s
                    ORDER BY {CITATION_DISPLAY_ORDER_SQL},
                             COALESCE(ce.citing_publication_date, p_citing.publication_date, '') DESC, ce.citing_paper_doi ASC
                    LIMIT 250;
                """
                cursor.execute(citations_query, [source, dataset_id])
                citations = _clean_paper_titles([dict(row) for row in cursor.fetchall()])

                return {
                    "dataset": dict(dataset),
                    "primary_papers": primary_papers,
                    "citations": citations,
                }
    except HTTPException:
        raise
    except psycopg.Error as e:
        logger.exception("Database query error in /api/paper-mapping/datasets/{source}/{dataset_id}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.exception("Unexpected error in /api/paper-mapping/datasets/{source}/{dataset_id}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/paper-mapping/citations")
async def get_paper_mapping_citations(
    source: Optional[str] = Query(None, description="Filter by source (CRCNS, DANDI, OpenNeuro, SPARC)"),
    dataset_id: Optional[str] = Query(None, description="Filter by dataset id"),
    limit: int = Query(50, ge=1, le=250),
    offset: int = Query(0, ge=0),
):
    source = _validate_paper_mapping_source(source)
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                _ensure_paper_mapping_tables(cursor)
                clauses: List[str] = []
                params: List[Any] = []
                if source:
                    clauses.append("ce.source = %s")
                    params.append(source)
                if dataset_id:
                    clauses.append("ce.dataset_id = %s")
                    params.append(dataset_id)
                where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

                count_query = f"""
                    {_paper_mapping_ctes(cursor)}
                    SELECT COUNT(*)::int AS total
                    FROM citation_edges ce
                    {where_sql};
                """
                cursor.execute(count_query, params)
                total = cursor.fetchone()["total"]

                c_text_status = (
                    "p_citing.text_status AS citing_text_status,"
                    if "text_status" in _table_columns(cursor, "papers")
                    else "NULL::text AS citing_text_status,"
                )
                query = f"""
                    {_paper_mapping_ctes(cursor)}
                    SELECT
                        ce.source,
                        ce.dataset_id,
                        ce.primary_paper_doi,
                        p_primary.title AS primary_paper_title,
                        ce.citing_paper_doi,
                        p_citing.title AS citing_paper_title,
                        {c_text_status}
                        ce.citing_publication_date,
                        ce.citation_source,
                        ce.citation_contexts,
                        ce.contexts_extracted_at,
                        COALESCE(NULLIF(cc.classification, ''), cc.status, 'unclassified') AS classification_status,
                        cc.classification,
                        cc.same_lab,
                        cc.same_lab_confidence,
                        cc.confidence,
                        cc.status,
                        cc.reasoning,
                        cc.mode,
                        cc.prompt_version,
                        cc.reuse_type,
                        cc.reuse_type_other,
                        cc.reused_modalities,
                        cc.source_archive,
                        cc.evidence_quotes,
                        cc.hallucinated_quote_count
                    FROM citation_edges ce
                    LEFT JOIN papers p_primary
                      ON p_primary.paper_doi = ce.primary_paper_doi
                    LEFT JOIN papers p_citing
                      ON p_citing.paper_doi = ce.citing_paper_doi
                    LEFT JOIN citation_classifications cc
                      ON cc.source = ce.source
                     AND cc.dataset_id = ce.dataset_id
                     AND cc.primary_paper_doi = ce.primary_paper_doi
                     AND cc.citing_paper_doi = ce.citing_paper_doi
                    {where_sql}
                    ORDER BY {CITATION_DISPLAY_ORDER_SQL},
                             COALESCE(ce.citing_publication_date, '') DESC, ce.citing_paper_doi ASC
                    LIMIT %s OFFSET %s;
                """
                cursor.execute(query, params + [limit, offset])
                return {"citations": _clean_paper_titles([dict(row) for row in cursor.fetchall()]), "count": total}
    except HTTPException:
        raise
    except psycopg.Error as e:
        logger.exception("Database query error in /api/paper-mapping/citations")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.exception("Unexpected error in /api/paper-mapping/citations")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")



@app.post("/api/refresh-view")
async def refresh_unified_view():
    """Manually create or refresh the unified_datasets view."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                result = create_unified_datasets_view(cursor)
                conn.commit()
                
        if not result.get("view_created", False):
            raise HTTPException(
                status_code=503, 
                detail="Cannot create view: No source tables (dandi_dataset or neuroscience_datasets) exist. Run the DAGs first to populate data."
            )
                
        return {
            "status": "success",
            "message": "unified_datasets view created/refreshed",
            "total_rows": result["total_rows"],
            "rows_by_source": result["rows_by_source"]
        }
    except HTTPException:
        raise
    except psycopg.Error as e:
        logger.error(f"Database error creating view: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error creating view: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/debug/view-info")
async def debug_view_info():
    """Debug endpoint to check view status and data sources."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Check if view exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.views 
                        WHERE table_schema = 'public' 
                        AND table_name = 'unified_datasets'
                    );
                """)
                view_exists = cursor.fetchone()[0]
                
                # Check table existence
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'dandi_dataset'
                    );
                """)
                dandi_exists = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'openneuro_dataset'
                    );
                """)
                openneuro_exists = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'neuroscience_datasets'
                    );
                """)
                neuro_exists = cursor.fetchone()[0]
                
                result = {
                    "unified_datasets_view_exists": view_exists,
                    "dandi_dataset_table_exists": dandi_exists,
                    "openneuro_dataset_table_exists": openneuro_exists,
                    "neuroscience_datasets_table_exists": neuro_exists,
                }
                
                if dandi_exists:
                    cursor.execute("SELECT COUNT(*) FROM dandi_dataset")
                    result["dandi_dataset_count"] = cursor.fetchone()[0]

                if openneuro_exists:
                    cursor.execute("SELECT COUNT(*) FROM openneuro_dataset")
                    result["openneuro_dataset_count"] = cursor.fetchone()[0]
                
                if neuro_exists:
                    cursor.execute("SELECT COUNT(*) FROM neuroscience_datasets")
                    result["neuroscience_datasets_count"] = cursor.fetchone()[0]
                
                if view_exists:
                    cursor.execute("SELECT COUNT(*) FROM unified_datasets")
                    result["unified_datasets_count"] = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT source, COUNT(*) as count 
                        FROM unified_datasets 
                        GROUP BY source 
                        ORDER BY source
                    """)
                    result["unified_datasets_by_source"] = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    # Get a sample of sources
                    cursor.execute("""
                        SELECT DISTINCT source 
                        FROM unified_datasets 
                        LIMIT 10
                    """)
                    result["sample_sources"] = [row[0] for row in cursor.fetchall()]
                
                return result
    except Exception as e:
        logger.error(f"Error in debug endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
