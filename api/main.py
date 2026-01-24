"""
Backend API service for NeuroD3 dataset discovery.
Provides REST endpoints to fetch neuroscience datasets from PostgreSQL.
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
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
                        citations,
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
                # Default ordering: newest first (published date), then citations
                query = f"{base_select}{filter_sql} ORDER BY created_at DESC NULLS LAST, citations DESC, title ASC LIMIT %s OFFSET %s"
                count_query = f"{base_count}{filter_sql}"

                cursor.execute(count_query, params)
                total = cursor.fetchone()["total"]

                cursor.execute(query, params + [limit, offset])
                datasets = cursor.fetchall()

                # Convert to list of dicts
                result = [dict(row) for row in datasets]

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

                # Facet counts
                # - by_source: apply modality filter (but not source)
                # - by_modality: apply source filter (but not modality)
                by_source_params = []
                by_source_where = sql.SQL("")
                if modalities:
                    by_source_where = sql.SQL("WHERE ") + sql.SQL(" AND ").join([sql.SQL("modality ILIKE %s")] * len(modalities))
                    by_source_params.extend([f"%{m}%" for m in modalities])

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
                if modalities:
                    for m in modalities:
                        total_where_clauses.append(sql.SQL("modality ILIKE %s"))
                        total_params.append(f"%{m}%")

                total_where = sql.SQL("")
                if total_where_clauses:
                    total_where = sql.SQL("WHERE ").join([sql.SQL(""), sql.SQL(" AND ").join(total_where_clauses)])

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
