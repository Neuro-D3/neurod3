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

                base_select = f"""
                    SELECT
                        source,
                        dataset_id as id,
                        title,
                        modality,
                        papers,
                        url,
                        description,
                        created_at,
                        updated_at
                    FROM {table_name}
                    WHERE 1=1
                """
                base_count = f"SELECT COUNT(*) as total FROM {table_name} WHERE 1=1"
                filters = []
                params = []

                if source:
                    if source not in ALLOWED_SOURCES:
                        raise HTTPException(status_code=400, detail=f"Invalid source: {source}")
                    filters.append("source = %s")
                    params.append(source)

                if modality:
                    modalities = [m.strip() for m in modality.split(",") if m.strip()]
                    for m in modalities:
                        filters.append("modality ILIKE %s")
                        params.append(f"%{m}%")

                if search:
                    filters.append("(title ILIKE %s OR description ILIKE %s)")
                    search_pattern = f"%{search}%"
                    params.extend([search_pattern, search_pattern])

                filter_sql = f" AND {' AND '.join(filters)}" if filters else ""

                # Server-side ordering (applies before pagination).
                sort_by_norm = (sort_by or "published").strip().lower()
                sort_order_norm = (sort_order or "desc").strip().lower()
                if sort_order_norm not in {"asc", "desc"}:
                    raise HTTPException(status_code=400, detail=f"Invalid sort_order: {sort_order}")

                sort_column_by_key = {
                    "published": "created_at",
                    "papers": "papers",
                    "title": "title",
                    "id": "dataset_id",
                    "source": "source",
                    "modality": "modality",
                }
                sort_col = sort_column_by_key.get(sort_by_norm)
                if not sort_col:
                    raise HTTPException(status_code=400, detail=f"Invalid sort_by: {sort_by}")

                # Keep ordering deterministic with tie-breakers.
                order_sql = f"{sort_col} {sort_order_norm.upper()} NULLS LAST, title ASC, dataset_id ASC"
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
                    by_source_clauses.append(sql.SQL("(title ILIKE %s OR description ILIKE %s)"))
                    by_source_params.extend([search_pattern, search_pattern])
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
                    by_modality_clauses.append(sql.SQL("(title ILIKE %s OR description ILIKE %s)"))
                    by_modality_params.extend([search_pattern, search_pattern])
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
                    total_where_clauses.append(sql.SQL("(title ILIKE %s OR description ILIKE %s)"))
                    total_params.extend([search_pattern, search_pattern])
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


@app.get("/api/paper-mapping/summary")
async def get_paper_mapping_summary(
    source: Optional[str] = Query(None, description="Filter by source (DANDI, OpenNeuro)"),
):
    source = _validate_paper_mapping_source(source)
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                _ensure_paper_mapping_tables(cursor)
                where_sql, params = _paper_mapping_filter_sql(source=source)
                query = f"""
                    {_paper_mapping_ctes()}
                    SELECT
                        COUNT(DISTINCT CASE WHEN map.paper_doi IS NOT NULL THEN db.source || ':' || db.dataset_id END)::int AS datasets_with_mapped_papers,
                        COUNT(DISTINCT map.paper_doi)::int AS distinct_mapped_primary_papers,
                        COUNT(DISTINCT ce.source || ':' || ce.dataset_id || ':' || ce.primary_paper_doi || ':' || ce.citing_paper_doi)::int AS citation_edges,
                        COUNT(DISTINCT CASE WHEN ce.contexts_extracted_at IS NOT NULL THEN ce.source || ':' || ce.dataset_id || ':' || ce.primary_paper_doi || ':' || ce.citing_paper_doi END)::int AS citations_with_contexts,
                        COALESCE(SUM({_count_contexts_expr('ce')}), 0)::int AS citation_context_count,
                        COUNT(DISTINCT CASE WHEN cc.classification IS NOT NULL THEN cc.source || ':' || cc.dataset_id || ':' || cc.primary_paper_doi || ':' || cc.citing_paper_doi END)::int AS classified_edges,
                        COUNT(DISTINCT CASE WHEN cc.status = 'placeholder' THEN cc.source || ':' || cc.dataset_id || ':' || cc.primary_paper_doi || ':' || cc.citing_paper_doi END)::int AS placeholder_classification_edges
                    FROM dataset_base db
                    LEFT JOIN dataset_map map
                      ON map.source = db.source AND map.dataset_id = db.dataset_id
                    LEFT JOIN citation_edges ce
                      ON ce.source = db.source AND ce.dataset_id = db.dataset_id
                    LEFT JOIN citation_classifications cc
                      ON cc.source = db.source AND cc.dataset_id = db.dataset_id
                    {where_sql};
                """
                cursor.execute(query, params)
                summary = dict(cursor.fetchone() or {})

                source_query = f"""
                    {_paper_mapping_ctes()}
                    SELECT
                        db.source,
                        COUNT(DISTINCT CASE WHEN map.paper_doi IS NOT NULL THEN db.source || ':' || db.dataset_id END)::int AS datasets_with_mapped_papers,
                        COUNT(DISTINCT map.paper_doi)::int AS distinct_mapped_primary_papers,
                        COUNT(DISTINCT ce.source || ':' || ce.dataset_id || ':' || ce.primary_paper_doi || ':' || ce.citing_paper_doi)::int AS citation_edges,
                        COUNT(DISTINCT CASE WHEN ce.contexts_extracted_at IS NOT NULL THEN ce.source || ':' || ce.dataset_id || ':' || ce.primary_paper_doi || ':' || ce.citing_paper_doi END)::int AS citations_with_contexts,
                        COUNT(DISTINCT CASE WHEN cc.classification IS NOT NULL THEN cc.source || ':' || cc.dataset_id || ':' || cc.primary_paper_doi || ':' || cc.citing_paper_doi END)::int AS classified_edges
                    FROM dataset_base db
                    LEFT JOIN dataset_map map
                      ON map.source = db.source AND map.dataset_id = db.dataset_id
                    LEFT JOIN citation_edges ce
                      ON ce.source = db.source AND ce.dataset_id = db.dataset_id
                    LEFT JOIN citation_classifications cc
                      ON cc.source = db.source AND cc.dataset_id = db.dataset_id
                    {where_sql}
                    GROUP BY db.source
                    ORDER BY db.source;
                """
                cursor.execute(source_query, params)
                by_source = [dict(row) for row in cursor.fetchall()]

                classification_query = f"""
                    {_paper_mapping_ctes()}
                    SELECT
                        COALESCE(NULLIF(cc.classification, ''), cc.status, 'unclassified') AS bucket,
                        COUNT(*)::int AS count
                    FROM citation_classifications cc
                    JOIN dataset_base db
                      ON db.source = cc.source AND db.dataset_id = cc.dataset_id
                    {where_sql.replace('db.', 'cc.')}
                    GROUP BY 1
                    ORDER BY count DESC, bucket ASC;
                """
                cursor.execute(classification_query, params)
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

    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                _ensure_paper_mapping_tables(cursor)
                where_sql, params = _paper_mapping_filter_sql(source=source, search=search)
                aggregated_cte = f"""
                    {_paper_mapping_ctes()},
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
                            COUNT(DISTINCT map.paper_doi)::int AS mapped_papers_count,
                            COUNT(DISTINCT ce.primary_paper_doi || ':' || ce.citing_paper_doi)::int AS citation_edges_count,
                            COUNT(DISTINCT CASE WHEN ce.contexts_extracted_at IS NOT NULL THEN ce.primary_paper_doi || ':' || ce.citing_paper_doi END)::int AS citations_with_contexts_count,
                            COALESCE(SUM({_count_contexts_expr('ce')}), 0)::int AS contexts_extracted_count,
                            MAX(p_primary.publication_date) AS latest_primary_publication_date,
                            MAX(ce.citing_publication_date) AS latest_citing_publication_date,
                            COUNT(DISTINCT CASE WHEN cc.classification IS NOT NULL THEN cc.primary_paper_doi || ':' || cc.citing_paper_doi END)::int AS classified_edges_count,
                            COUNT(DISTINCT CASE WHEN cc.status = 'placeholder' THEN cc.primary_paper_doi || ':' || cc.citing_paper_doi END)::int AS placeholder_classification_edges_count
                        FROM dataset_base db
                        LEFT JOIN dataset_map map
                          ON map.source = db.source AND map.dataset_id = db.dataset_id
                        LEFT JOIN papers p_primary
                          ON p_primary.paper_doi = map.paper_doi
                        LEFT JOIN citation_edges ce
                          ON ce.source = db.source AND ce.dataset_id = db.dataset_id
                        LEFT JOIN citation_classifications cc
                          ON cc.source = db.source AND cc.dataset_id = db.dataset_id
                        {where_sql}
                        GROUP BY
                            db.source, db.dataset_id, db.dataset_title, db.dataset_description,
                            db.modality, db.papers_count, db.created_at, db.updated_at, db.url
                    )
                """
                count_query = f"{aggregated_cte} SELECT COUNT(*)::int AS total FROM aggregated;"
                cursor.execute(count_query, params)
                total = cursor.fetchone()["total"]

                query = f"""
                    {aggregated_cte}
                    SELECT *
                    FROM aggregated
                    ORDER BY {sort_col} {sort_order_norm.upper()} NULLS LAST, dataset_title ASC, dataset_id ASC
                    LIMIT %s OFFSET %s;
                """
                cursor.execute(query, params + [limit, offset])
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
                        COUNT(DISTINCT map.paper_doi)::int AS mapped_papers_count,
                        COUNT(DISTINCT ce.primary_paper_doi || ':' || ce.citing_paper_doi)::int AS citation_edges_count,
                        COUNT(DISTINCT CASE WHEN ce.contexts_extracted_at IS NOT NULL THEN ce.primary_paper_doi || ':' || ce.citing_paper_doi END)::int AS citations_with_contexts_count,
                        COALESCE(SUM({_count_contexts_expr('ce')}), 0)::int AS contexts_extracted_count
                    FROM dataset_base db
                    LEFT JOIN dataset_map map
                      ON map.source = db.source AND map.dataset_id = db.dataset_id
                    LEFT JOIN citation_edges ce
                      ON ce.source = db.source AND ce.dataset_id = db.dataset_id
                    WHERE db.source = %s AND db.dataset_id = %s
                    GROUP BY
                        db.source, db.dataset_id, db.dataset_title, db.dataset_description,
                        db.modality, db.papers_count, db.created_at, db.updated_at, db.url;
                """
                cursor.execute(base_query, [source, dataset_id])
                dataset = cursor.fetchone()
                if not dataset:
                    raise HTTPException(status_code=404, detail="Dataset not found in paper mapping tables")

                primary_papers_query = f"""
                    {_paper_mapping_ctes()}
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
                        COUNT(DISTINCT ce.citing_paper_doi)::int AS citing_papers_count,
                        COUNT(DISTINCT CASE WHEN ce.contexts_extracted_at IS NOT NULL THEN ce.citing_paper_doi END)::int AS citations_with_contexts_count,
                        COUNT(DISTINCT CASE WHEN cc.classification IS NOT NULL THEN cc.citing_paper_doi END)::int AS classified_edges_count,
                        COUNT(DISTINCT CASE WHEN cc.status = 'placeholder' THEN cc.citing_paper_doi END)::int AS placeholder_classification_edges_count
                    FROM dataset_map map
                    LEFT JOIN papers p
                      ON p.paper_doi = map.paper_doi
                    LEFT JOIN citation_edges ce
                      ON ce.source = map.source AND ce.dataset_id = map.dataset_id AND ce.primary_paper_doi = map.paper_doi
                    LEFT JOIN citation_classifications cc
                      ON cc.source = map.source AND cc.dataset_id = map.dataset_id AND cc.primary_paper_doi = map.paper_doi
                    WHERE map.source = %s AND map.dataset_id = %s
                    GROUP BY
                        map.paper_doi, map.doi_source, map.relation_type, map.resolved_at, map.run_id,
                        p.title, p.authors, p.openalex_id, p.publication_date, p.publication_year
                    ORDER BY COALESCE(p.publication_date, '') DESC, map.paper_doi ASC;
                """
                cursor.execute(primary_papers_query, [source, dataset_id])
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
                        COALESCE(cc.classification, cc.status, 'unclassified') AS classification_status,
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
                        COALESCE(cc.classification, cc.status, 'unclassified') AS classification_status,
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
