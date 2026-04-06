"""
Backend API service for NeuroD3 dataset discovery.
Provides REST endpoints to fetch neuroscience datasets from PostgreSQL.
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, Dict, List, Optional
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import os
import sys
from pathlib import Path
from contextlib import contextmanager
import logging

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
ALLOWED_SOURCES = {"DANDI", "Kaggle", "OpenNeuro", "PhysioNet"}
ALLOWED_PAPER_MAPPING_SOURCES = {"DANDI", "OpenNeuro"}

# CORS configuration to allow frontend to access the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://frontend:3000"],
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


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = None
    try:
        conn = psycopg.connect(DB_CONNINFO)
        yield conn
    except psycopg.Error as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")
    finally:
        if conn:
            conn.close()


def _validate_paper_mapping_source(source: Optional[str]) -> Optional[str]:
    if source is None:
        return None
    if source not in ALLOWED_PAPER_MAPPING_SOURCES:
        raise HTTPException(status_code=400, detail=f"Invalid paper mapping source: {source}")
    return source


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


def _paper_mapping_ctes() -> str:
    return """
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
            c.run_id
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
            c.run_id
        FROM openneuro_paper_citation_classifications c
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
                        "unified_datasets_view": "exists",
                        "view_row_count": view_count
                    }
                else:
                    return {
                        "status": "healthy", 
                        "database": "connected",
                        "unified_datasets_view": "missing"
                    }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail={"status": "unhealthy", "database": "disconnected", "error": str(e)}
        )


@app.get("/api/datasets")
async def get_datasets(
    source: Optional[str] = Query(None, description="Filter by source (DANDI, Kaggle, OpenNeuro, PhysioNet)"),
    modality: Optional[str] = Query(None, description="Filter by modality (comma-separated for AND)"),
    search: Optional[str] = Query(None, description="Search in title and description"),
    sort_by: str = Query("published", description="Sort column (published, papers, title, id, source, modality)"),
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
                      AND column_name IN ('authors', 'num_subjects')
                """, (table_name,))
                ds_opt_cols = {r["column_name"] for r in cursor.fetchall()}
                authors_expr = "authors," if "authors" in ds_opt_cols else "NULL::jsonb AS authors,"
                num_subjects_expr = "num_subjects," if "num_subjects" in ds_opt_cols else "NULL::integer AS num_subjects,"

                secondary_reuse_subquery = """
                    COALESCE((
                        SELECT COUNT(DISTINCT citing_paper_doi)::int
                        FROM dandi_paper_citation_classifications
                        WHERE dandi_id = d.dataset_id AND classification = 'SECONDARY'
                    ), 0)
                    +
                    COALESCE((
                        SELECT COUNT(DISTINCT citing_paper_doi)::int
                        FROM openneuro_paper_citation_classifications
                        WHERE openneuro_id = d.dataset_id AND classification = 'SECONDARY'
                    ), 0)
                """

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
                        d.updated_at,
                        ({secondary_reuse_subquery}) AS secondary_reuse_count
                    FROM {table_name} d
                    WHERE 1=1
                """
                base_count = f"SELECT COUNT(*) as total FROM {table_name} d WHERE 1=1"
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
                sort_by_norm = (sort_by or "published").strip().lower()
                sort_order_norm = (sort_order or "desc").strip().lower()
                if sort_order_norm not in {"asc", "desc"}:
                    raise HTTPException(status_code=400, detail=f"Invalid sort_order: {sort_order}")

                sort_column_by_key = {
                    "published": "d.created_at",
                    "papers": "(COALESCE(d.papers, 0) + secondary_reuse_count)",
                    "title": "d.title",
                    "id": "d.dataset_id",
                    "source": "d.source",
                    "modality": "d.modality",
                }
                sort_col = sort_column_by_key.get(sort_by_norm)
                if not sort_col:
                    raise HTTPException(status_code=400, detail=f"Invalid sort_by: {sort_by}")

                # Keep ordering deterministic with tie-breakers.
                order_sql = f"{sort_col} {sort_order_norm.upper()} NULLS LAST, d.title ASC, d.dataset_id ASC"
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
                by_source_clauses = []
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
                by_modality_clauses = []
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
                total_where_clauses = []
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


@app.get("/api/datasets/{dataset_id:path}")
async def get_dataset_detail(dataset_id: str):
    """
    Fetch a single dataset by its archive-native ID (e.g. 000003 for DANDI, ds000001
    for OpenNeuro), including associated primary papers and citing papers when available.
    """
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
                optional_cols = ["full_description", "authors", "contributors", "license", "num_subjects"]
                cursor.execute(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name = %s;""",
                    (table_name,),
                )
                existing_cols = {row["column_name"] for row in cursor.fetchall()}
                select_cols = base_cols + [c for c in optional_cols if c in existing_cols]

                cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in select_cols)
                detail_query = sql.SQL(
                    "SELECT {cols} FROM {table} WHERE dataset_id = %s LIMIT 1;"
                ).format(cols=cols_sql, table=sql.Identifier(table_name))
                cursor.execute(detail_query, (dataset_id,))
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
                          AND column_name IN ('journal', 'senior_author_country')
                    """)
                    paper_opt_cols = {r["column_name"] for r in cursor.fetchall()}
                    p_journal = "p.journal," if "journal" in paper_opt_cols else "NULL AS journal,"
                    p_country = "p.senior_author_country," if "senior_author_country" in paper_opt_cols else "NULL AS senior_author_country,"

                    primary_papers_query = f"""
                        {_paper_mapping_ctes()}
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
                    primary_papers = [dict(r) for r in cursor.fetchall()]

                    c_journal = "p_citing.journal AS citing_journal," if "journal" in paper_opt_cols else "NULL AS citing_journal,"
                    c_country = "p_citing.senior_author_country AS citing_senior_author_country," if "senior_author_country" in paper_opt_cols else "NULL AS citing_senior_author_country,"

                    citations_query = f"""
                        {_paper_mapping_ctes()}
                        SELECT
                            ce.primary_paper_doi,
                            p_primary.title AS primary_paper_title,
                            ce.citing_paper_doi,
                            p_citing.title AS citing_paper_title,
                            p_citing.authors AS citing_authors,
                            {c_journal}
                            {c_country}
                            p_citing.publication_date AS citing_publication_date,
                            p_citing.publication_year AS citing_publication_year,
                            COALESCE(NULLIF(cc.classification, ''), cc.status, 'unclassified') AS classification_status,
                            cc.classification,
                            cc.confidence,
                            cc.reasoning
                        FROM citation_edges ce
                        LEFT JOIN papers p_primary ON p_primary.paper_doi = ce.primary_paper_doi
                        LEFT JOIN papers p_citing ON p_citing.paper_doi = ce.citing_paper_doi
                        LEFT JOIN citation_classifications cc
                          ON cc.source = ce.source
                         AND cc.dataset_id = ce.dataset_id
                         AND cc.primary_paper_doi = ce.primary_paper_doi
                         AND cc.citing_paper_doi = ce.citing_paper_doi
                        WHERE ce.source = %s AND ce.dataset_id = %s
                        ORDER BY COALESCE(p_citing.publication_date, '') DESC,
                                 ce.citing_paper_doi ASC
                        LIMIT 250;
                    """
                    cursor.execute(citations_query, [source, dataset_id])
                    citations = [dict(r) for r in cursor.fetchall()]

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


@app.get("/api/paper-mapping/summary")
async def get_paper_mapping_summary(
    source: Optional[str] = Query(None, description="Filter by source (DANDI, OpenNeuro)"),
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
                        **map_row, **cit_row, **cls_row,
                    }

                source_configs = []
                if not source or source == "DANDI":
                    source_configs.append(("DANDI", "dandi_paper_map", "dandi_id",
                                           "dandi_paper_citations", "dandi_paper_citation_classifications"))
                if not source or source == "OpenNeuro":
                    source_configs.append(("OpenNeuro", "openneuro_paper_map", "openneuro_id",
                                           "openneuro_paper_citations", "openneuro_paper_citation_classifications"))

                by_source = [_per_source_summary(*cfg) for cfg in source_configs]

                # Build overall summary by summing per-source values.
                # distinct_mapped_primary_papers needs dedup across sources
                # (a DOI could appear in both maps).
                all_dois_parts = []
                if not source or source == "DANDI":
                    all_dois_parts.append("SELECT DISTINCT paper_doi FROM dandi_paper_map")
                if not source or source == "OpenNeuro":
                    all_dois_parts.append("SELECT DISTINCT paper_doi FROM openneuro_paper_map")
                if all_dois_parts:
                    cursor.execute(f"SELECT COUNT(DISTINCT paper_doi)::int AS n FROM ({' UNION ALL '.join(all_dois_parts)}) t;")
                    distinct_papers = (cursor.fetchone() or {}).get("n", 0)
                else:
                    distinct_papers = 0

                summary = {
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
    source: Optional[str] = Query(None, description="Filter by source (DANDI, OpenNeuro)"),
    search: Optional[str] = Query(None, description="Search in dataset id, title, and description"),
    classification_bucket: Optional[str] = Query(
        None,
        description=(
            "Only datasets with at least one citation edge whose bucket matches "
            "COALESCE(NULLIF(classification,''), status, 'unclassified') — same keys as summary by_classification "
            "(e.g. SECONDARY, NEITHER, placeholder, classified, error)."
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
                    {_paper_mapping_ctes()},
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
                    {_paper_mapping_ctes()}
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
                    {_paper_mapping_ctes()},
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
                primary_papers = [dict(row) for row in cursor.fetchall()]

                citations_query = f"""
                    {_paper_mapping_ctes()}
                    SELECT
                        ce.primary_paper_doi,
                        p_primary.title AS primary_paper_title,
                        ce.citing_paper_doi,
                        p_citing.title AS citing_paper_title,
                        p_citing.authors AS citing_authors,
                        p_citing.publication_date AS citing_publication_date_from_papers,
                        p_citing.publication_year AS citing_publication_year,
                        ce.citing_publication_date,
                        ce.citation_source,
                        ce.matched_primary_paper_doi,
                        ce.matched_primary_openalex_id,
                        ce.citation_contexts,
                        ce.contexts_extracted_at,
                        COALESCE(NULLIF(cc.classification, ''), cc.status, 'unclassified') AS classification_status,
                        cc.classification,
                        cc.same_lab,
                        cc.confidence,
                        cc.status,
                        cc.reasoning,
                        cc.classification_model,
                        cc.classified_at
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
                    ORDER BY COALESCE(ce.citing_publication_date, p_citing.publication_date, '') DESC, ce.citing_paper_doi ASC
                    LIMIT 250;
                """
                cursor.execute(citations_query, [source, dataset_id])
                citations = [dict(row) for row in cursor.fetchall()]

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
    source: Optional[str] = Query(None, description="Filter by source (DANDI, OpenNeuro)"),
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
                    {_paper_mapping_ctes()}
                    SELECT COUNT(*)::int AS total
                    FROM citation_edges ce
                    {where_sql};
                """
                cursor.execute(count_query, params)
                total = cursor.fetchone()["total"]

                query = f"""
                    {_paper_mapping_ctes()}
                    SELECT
                        ce.source,
                        ce.dataset_id,
                        ce.primary_paper_doi,
                        p_primary.title AS primary_paper_title,
                        ce.citing_paper_doi,
                        p_citing.title AS citing_paper_title,
                        ce.citing_publication_date,
                        ce.citation_source,
                        ce.citation_contexts,
                        ce.contexts_extracted_at,
                        COALESCE(NULLIF(cc.classification, ''), cc.status, 'unclassified') AS classification_status,
                        cc.classification,
                        cc.same_lab,
                        cc.confidence,
                        cc.status,
                        cc.reasoning
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
                    ORDER BY COALESCE(ce.citing_publication_date, '') DESC, ce.citing_paper_doi ASC
                    LIMIT %s OFFSET %s;
                """
                cursor.execute(query, params + [limit, offset])
                return {"citations": [dict(row) for row in cursor.fetchall()], "count": total}
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
