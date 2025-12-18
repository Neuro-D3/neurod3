"""
Backend API service for NeuroD3 dataset discovery.
Provides REST endpoints to fetch neuroscience datasets from PostgreSQL.
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import psycopg
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
                cursor.execute("SELECT COUNT(*) FROM unified_datasets")
                count = cursor.fetchone()[0]
                return {
                    "status": "healthy",
                    "database": "connected",
                    "dataset_count": count
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

                return {
                    "datasets": [dict(row) for row in datasets],
                    "count": len(datasets)
                }

    except psycopg.Error as e:
        logger.exception("Database query error")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


@app.get("/api/datasets/stats")
async def get_dataset_stats():
    """
    Get statistics about datasets in the database.
    Returns counts by source and modality.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                # Get counts by source
                cursor.execute("""
                    SELECT source, COUNT(*) as count
                    FROM unified_datasets
                    GROUP BY source
                    ORDER BY count DESC
                """)
                by_source = {row['source']: row['count'] for row in cursor.fetchall()}

                # Get counts by modality
                cursor.execute("""
                    SELECT modality, COUNT(*) as count
                    FROM unified_datasets
                    GROUP BY modality
                    ORDER BY count DESC
                """)
                by_modality = {row['modality']: row['count'] for row in cursor.fetchall()}

                # Get total count
                cursor.execute("SELECT COUNT(*) as total FROM unified_datasets")
                total = cursor.fetchone()['total']

                return {
                    "total": total,
                    "by_source": by_source,
                    "by_modality": by_modality
                }

    except psycopg.Error as e:
        logger.exception("Database query error in /api/datasets/stats")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
