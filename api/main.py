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
from contextlib import contextmanager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NeuroD3 API", version="1.0.0")

# Allowed filter values
ALLOWED_SOURCES = {"DANDI", "Kaggle", "OpenNeuro", "PhysioNet"}
ALLOWED_MODALITIES = {
    "Behavioral",
    "Calcium Imaging",
    "Clinical",
    "ECG",
    "EEG",
    "Electrophysiology",
    "fMRI",
    "MRI",
    "Survey",
    "X-ray",
}

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
    modality: Optional[str] = Query(None, description="Filter by modality"),
    search: Optional[str] = Query(None, description="Search in title and description"),
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
                view_exists = bool(
                    view_row[0] if isinstance(view_row, tuple) else view_row.get("exists") or list(view_row.values())[0]
                )
                
                # Check if neuroscience_datasets table exists (fallback target)
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'neuroscience_datasets'
                    );
                """)
                neuro_row = cursor.fetchone()
                neuro_table_exists = bool(
                    neuro_row[0] if isinstance(neuro_row, tuple) else neuro_row.get("exists") or list(neuro_row.values())[0]
                )
                
                if not view_exists and not neuro_table_exists:
                    # Database is missing both view and base table (likely fresh or reset DB)
                    raise HTTPException(
                        status_code=503,
                        detail="No dataset tables or view found. Run the populate_neuroscience_datasets and dandi_ingestion DAGs or POST /api/refresh-view after tables exist."
                    )
                
                # Build dynamic query based on filters
                if view_exists:
                    logger.info("Using unified_datasets view for query")
                    query = """
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
                        FROM unified_datasets
                        WHERE 1=1
                    """
                else:
                    # Fallback to neuroscience_datasets table if view doesn't exist
                    logger.warning("unified_datasets view does not exist, falling back to neuroscience_datasets table")
                    query = """
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
                        FROM neuroscience_datasets
                        WHERE 1=1
                    """
                params = []

                if source:
                    if source not in ALLOWED_SOURCES:
                        raise HTTPException(status_code=400, detail=f"Invalid source: {source}")
                    query += " AND source = %s"
                    params.append(source)

                if modality:
                    if modality not in ALLOWED_MODALITIES:
                        raise HTTPException(status_code=400, detail=f"Invalid modality: {modality}")
                    query += " AND modality = %s"
                    params.append(modality)

                if search:
                    query += " AND (title ILIKE %s OR description ILIKE %s)"
                    search_pattern = f"%{search}%"
                    params.extend([search_pattern, search_pattern])

                query += " ORDER BY citations DESC, title ASC"

                cursor.execute(query, params)
                datasets = cursor.fetchall()

                # Convert to list of dicts
                result = [dict(row) for row in datasets]

                return {
                    "datasets": result,
                    "count": len(result)
                }

    except psycopg.Error as e:
        logger.exception("Database query error")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.exception("Unexpected error in /api/datasets")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/datasets/stats")
async def get_dataset_stats():
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
                view_exists = cursor.fetchone()[0]
                
                # Whitelist of allowed table/view names for safety
                ALLOWED_TABLE_NAMES = {"unified_datasets", "neuroscience_datasets"}
                
                table_name = "unified_datasets" if view_exists else "neuroscience_datasets"
                if not view_exists:
                    logger.warning("unified_datasets view does not exist, using neuroscience_datasets table for stats")
                
                # Validate table name against whitelist
                if table_name not in ALLOWED_TABLE_NAMES:
                    raise ValueError(f"Invalid table name: {table_name}")
                
                # Ensure the chosen table/view actually exists before querying
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    ) OR EXISTS (
                        SELECT FROM information_schema.views 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, (table_name, table_name))
                target_exists = cursor.fetchone()[0]
                if not target_exists:
                    raise HTTPException(
                        status_code=503,
                        detail="Dataset view/table not found. Run the populate_neuroscience_datasets and dandi_ingestion DAGs or POST /api/refresh-view after tables exist."
                    )
                
                # Use psycopg.sql.Identifier() for safe table name construction
                table_identifier = sql.Identifier(table_name)
                
                # Get counts by source
                query_source = sql.SQL("""
                    SELECT source, COUNT(*) as count
                    FROM {}
                    GROUP BY source
                    ORDER BY count DESC
                """).format(table_identifier)
                cursor.execute(query_source)
                by_source = {row['source']: row['count'] for row in cursor.fetchall()}

                # Get counts by modality
                query_modality = sql.SQL("""
                    SELECT modality, COUNT(*) as count
                    FROM {}
                    GROUP BY modality
                    ORDER BY count DESC
                """).format(table_identifier)
                cursor.execute(query_modality)
                by_modality = {row['modality']: row['count'] for row in cursor.fetchall()}

                # Get total count
                query_total = sql.SQL("SELECT COUNT(*) as total FROM {}").format(table_identifier)
                cursor.execute(query_total)
                total = cursor.fetchone()['total']

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


def _create_unified_datasets_view_api(cursor):
    """
    Create or replace the unified_datasets view (API version using psycopg).
    
    NOTE: This function should match the logic in dags/utils/database.py::create_unified_datasets_view()
    but is kept here as a local implementation since the API uses psycopg (not psycopg2).
    If the view creation logic changes, update both locations to maintain consistency.
    """
    # Check if tables exist
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'dandi_dataset'
        );
    """)
    dandi_table_exists = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'neuroscience_datasets'
        );
    """)
    neuro_table_exists = cursor.fetchone()[0]
    
    if not dandi_table_exists and not neuro_table_exists:
        raise ValueError("Neither dandi_dataset nor neuroscience_datasets tables exist")
    
    # Build view SQL based on which tables exist
    if dandi_table_exists and neuro_table_exists:
        create_view_sql = """
        CREATE OR REPLACE VIEW unified_datasets AS
        SELECT
            'DANDI'::text AS source,
            dataset_id,
            title,
            modality,
            citations,
            url,
            description,
            created_at,
            updated_at,
            version
        FROM dandi_dataset
        
        UNION ALL
        
        SELECT
            source::text,
            dataset_id,
            title,
            modality,
            citations,
            url,
            description,
            created_at,
            updated_at,
            NULL::VARCHAR(64) AS version
        FROM neuroscience_datasets
        WHERE source != 'DANDI';
        """
    elif dandi_table_exists:
        create_view_sql = """
        CREATE OR REPLACE VIEW unified_datasets AS
        SELECT
            'DANDI'::text AS source,
            dataset_id,
            title,
            modality,
            citations,
            url,
            description,
            created_at,
            updated_at,
            version
        FROM dandi_dataset;
        """
    else:
        create_view_sql = """
        CREATE OR REPLACE VIEW unified_datasets AS
        SELECT
            source::text,
            dataset_id,
            title,
            modality,
            citations,
            url,
            description,
            created_at,
            updated_at,
            NULL::VARCHAR(64) AS version
        FROM neuroscience_datasets;
        """
    
    cursor.execute(create_view_sql)
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM unified_datasets")
    total_count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT source, COUNT(*) as count 
        FROM unified_datasets 
        GROUP BY source 
        ORDER BY source
    """)
    source_counts = {row[0]: row[1] for row in cursor.fetchall()}
    
    return {
        "total_rows": total_count,
        "rows_by_source": source_counts,
    }


@app.post("/api/refresh-view")
async def refresh_unified_view():
    """Manually create or refresh the unified_datasets view."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                result = _create_unified_datasets_view_api(cursor)
                conn.commit()
                
        return {
            "status": "success",
            "message": "unified_datasets view created/refreshed",
            "total_rows": result["total_rows"],
            "rows_by_source": result["rows_by_source"]
        }
    except ValueError as e:
        logger.error(f"Error creating view: {e}")
        raise HTTPException(status_code=400, detail=str(e))
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
                        AND table_name = 'neuroscience_datasets'
                    );
                """)
                neuro_exists = cursor.fetchone()[0]
                
                result = {
                    "unified_datasets_view_exists": view_exists,
                    "dandi_dataset_table_exists": dandi_exists,
                    "neuroscience_datasets_table_exists": neuro_exists,
                }
                
                if dandi_exists:
                    cursor.execute("SELECT COUNT(*) FROM dandi_dataset")
                    result["dandi_dataset_count"] = cursor.fetchone()[0]
                
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
