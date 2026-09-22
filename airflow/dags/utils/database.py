"""
Database utility functions for Airflow DAGs.
This is a utility module, not a DAG file.
"""
# Prevent this file from being treated as a DAG
# Airflow will skip files that don't define a 'dag' object

from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Handle imports for both relative (module) and absolute (Airflow direct import) cases
try:
    # Try relative import first (when imported as a module)
    from .environment import get_database_config, get_database_connection_string
except (ImportError, ValueError):
    try:
        # Try absolute import (when Airflow imports directly)
        from utils.environment import get_database_config, get_database_connection_string
    except ImportError:
        # Last resort: import from the same directory
        import sys
        import os
        # Add parent directory to path if not already there
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        from utils.environment import get_database_config, get_database_connection_string


@contextmanager
def get_db_connection():
    """
    Context manager for PostgreSQL connection.
    
    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()
    """
    config = get_database_config()
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password']
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_db_session():
    """
    Context manager for SQLAlchemy session.
    
    Usage:
        with get_db_session() as session:
            result = session.execute(text("SELECT * FROM table"))
            rows = result.fetchall()
    """
    connection_string = get_database_connection_string()
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def execute_query(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query and return results as a list of dictionaries.
    
    Args:
        query: SQL query string
        params: Optional query parameters
        
    Returns:
        List of dictionaries representing rows
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params or {})
            return [dict(row) for row in cursor.fetchall()]


def _convert_proxy_to_value(value: Any) -> Any:
    """
    Convert Airflow Proxy objects to their actual Python values.
    
    Args:
        value: Value that might be a Proxy object
        
    Returns:
        Actual Python value
    """
    if value is None:
        return None
    
    # Check if it's already a basic Python type
    if isinstance(value, (str, int, float, bool, datetime)):
        return value
    
    # Check if it's a Proxy object by examining the type string
    type_str = str(type(value))
    if 'Proxy' not in type_str and 'Lazy' not in type_str:
        # Not a Proxy, return as-is
        return value
    
    # Try to extract the actual value from Proxy objects
    try:
        # Method 1: Try accessing common Proxy attributes
        if hasattr(value, '_proxied'):
            return _convert_proxy_to_value(getattr(value, '_proxied'))
        if hasattr(value, '__wrapped__'):
            return _convert_proxy_to_value(getattr(value, '__wrapped__'))
        if hasattr(value, '_wrapped'):
            return _convert_proxy_to_value(getattr(value, '_wrapped'))
        
        # Method 2: Try calling it (some Proxies are callable)
        if callable(value) and not isinstance(value, type):
            try:
                result = value()
                if result is not value:  # Avoid infinite recursion
                    return _convert_proxy_to_value(result)
            except:
                pass
        
        # Method 3: Try common value attributes
        for attr in ['value', '_value', 'obj', '_obj']:
            if hasattr(value, attr):
                try:
                    result = getattr(value, attr)
                    if result is not value:
                        return _convert_proxy_to_value(result)
                except:
                    pass
        
        # Method 4: For datetime-like Proxies, try to convert to string then parse
        if hasattr(value, 'isoformat'):
            try:
                iso_str = value.isoformat()
                # Try to parse it back as datetime
                return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
            except:
                pass
        
    except Exception as e:
        # If all else fails, log and return a safe default
        import logging
        logging.warning(f"Could not convert Proxy object {type(value)}: {e}")
    
    # Last resort: return the value and let psycopg2 try to handle it
    # This might still fail, but it's better than nothing
    return value


def execute_update(query: str, params: Optional[Dict[str, Any]] = None) -> int:
    """
    Execute an INSERT, UPDATE, or DELETE query.
    
    Args:
        query: SQL query string
        params: Optional query parameters (Proxy objects will be converted)
        
    Returns:
        Number of affected rows
    """
    # Convert any Proxy objects in params to actual values
    if params:
        converted_params = {k: _convert_proxy_to_value(v) for k, v in params.items()}
    else:
        converted_params = {}
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, converted_params)
            return cursor.rowcount


def create_table_if_not_exists(table_name: str, schema: str) -> None:
    """
    Create a table if it doesn't exist.
    
    Args:
        table_name: Name of the table
        schema: SQL schema definition
    """
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {schema}
    )
    """
    execute_update(query)


def create_unified_datasets_view(cursor) -> Dict[str, Any]:
    """
    Create or replace the unified_datasets view that combines available dataset source tables.
    
    This function checks which tables exist and creates the appropriate view SQL.
    Works with both psycopg2 and psycopg cursor objects.
    
    Args:
        cursor: Database cursor object (from psycopg or psycopg2)
        
    Returns:
        Dictionary with view creation status and statistics:
        - view_created: bool - Whether view was created/replaced
        - total_rows: int - Total rows in the view
        - rows_by_source: dict - Row counts by source
        - dandi_table_exists: bool
        - openneuro_table_exists: bool
        - neuro_table_exists: bool
    """
    import logging
    logger = logging.getLogger(__name__)
    
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
            AND table_name = 'openneuro_dataset'
        );
    """)
    openneuro_table_exists = cursor.fetchone()[0]

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'crcns_dataset'
        );
    """)
    crcns_table_exists = cursor.fetchone()[0]

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'sparc_dataset'
        );
    """)
    sparc_table_exists = cursor.fetchone()[0]

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'neuroscience_datasets'
        );
    """)
    neuro_table_exists = cursor.fetchone()[0]

    if not dandi_table_exists and not openneuro_table_exists and not crcns_table_exists and not sparc_table_exists and not neuro_table_exists:
        logger.warning(
            "No dataset source tables exist (dandi_dataset, openneuro_dataset, crcns_dataset, sparc_dataset, neuroscience_datasets). Cannot create view."
        )
        return {
            "view_created": False,
            "total_rows": 0,
            "rows_by_source": {},
            "dandi_table_exists": False,
            "openneuro_table_exists": False,
            "crcns_table_exists": False,
            "sparc_table_exists": False,
            "neuro_table_exists": False,
        }
    
    # Check which optional columns have been added (they may not exist yet if only
    # one ingestion DAG has run since the schema upgrade).
    def _has_column(table: str, column: str) -> bool:
        cursor.execute(
            """SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
            );""",
            (table, column),
        )
        return cursor.fetchone()[0]

    def _col_or_null(table: str, column: str, pg_type: str) -> str:
        if _has_column(table, column):
            return column
        return f"NULL::{pg_type} AS {column}"

    # Build view SQL based on which tables exist
    selects: List[str] = []

    if dandi_table_exists:
        fd = _col_or_null("dandi_dataset", "full_description", "text")
        au = _col_or_null("dandi_dataset", "authors", "jsonb")
        co = _col_or_null("dandi_dataset", "contributors", "jsonb")
        li = _col_or_null("dandi_dataset", "license", "text")
        ns = _col_or_null("dandi_dataset", "num_subjects", "integer")
        selects.append(f"""
        SELECT
            'DANDI'::text AS source,
            dataset_id, title, modality, papers, url, description,
            {fd},
            {au},
            {co},
            {li},
            {ns},
            created_at, updated_at
        FROM dandi_dataset
        """.strip())

    if openneuro_table_exists:
        fd = _col_or_null("openneuro_dataset", "full_description", "text")
        au = _col_or_null("openneuro_dataset", "authors", "jsonb")
        co = _col_or_null("openneuro_dataset", "contributors", "jsonb")
        li = _col_or_null("openneuro_dataset", "license", "text")
        ns = _col_or_null("openneuro_dataset", "num_subjects", "integer")
        selects.append(f"""
        SELECT
            'OpenNeuro'::text AS source,
            dataset_id, title, modality, papers, url, description,
            {fd},
            {au},
            {co},
            {li},
            {ns},
            created_at, updated_at
        FROM openneuro_dataset
        """.strip())

    if crcns_table_exists:
        fd = _col_or_null("crcns_dataset", "full_description", "text")
        au = _col_or_null("crcns_dataset", "authors", "jsonb")
        co = _col_or_null("crcns_dataset", "contributors", "jsonb")
        li = _col_or_null("crcns_dataset", "license", "text")
        ns = _col_or_null("crcns_dataset", "num_subjects", "integer")
        selects.append(f"""
        SELECT
            'CRCNS'::text AS source,
            dataset_id, title, modality, papers, url, description,
            {fd},
            {au},
            {co},
            {li},
            {ns},
            created_at, updated_at
        FROM crcns_dataset
        """.strip())

    if sparc_table_exists:
        fd = _col_or_null("sparc_dataset", "full_description", "text")
        au = _col_or_null("sparc_dataset", "authors", "jsonb")
        co = _col_or_null("sparc_dataset", "contributors", "jsonb")
        li = _col_or_null("sparc_dataset", "license", "text")
        ns = _col_or_null("sparc_dataset", "num_subjects", "integer")
        selects.append(f"""
        SELECT
            'SPARC'::text AS source,
            dataset_id, title, modality, papers, url, description,
            {fd},
            {au},
            {co},
            {li},
            {ns},
            created_at, updated_at
        FROM sparc_dataset
        """.strip())

    if neuro_table_exists:
        excluded_sources = ["'DANDI'", "'OpenNeuro'", "'CRCNS'", "'SPARC'"]
        if not dandi_table_exists:
            excluded_sources.remove("'DANDI'")
        if not openneuro_table_exists:
            excluded_sources.remove("'OpenNeuro'")
        if not crcns_table_exists:
            excluded_sources.remove("'CRCNS'")
        if not sparc_table_exists:
            excluded_sources.remove("'SPARC'")
        if excluded_sources:
            where_clause = f"WHERE source NOT IN ({', '.join(excluded_sources)})"
        else:
            where_clause = ""
        selects.append(f"""
        SELECT
            source::text,
            dataset_id, title, modality, papers, url, description,
            NULL::text AS full_description,
            NULL::jsonb AS authors,
            NULL::jsonb AS contributors,
            NULL::text AS license,
            NULL::integer AS num_subjects,
            created_at, updated_at
        FROM neuroscience_datasets
        {where_clause}
        """.strip())

    # Join whichever sources exist
    create_view_sql = "CREATE OR REPLACE VIEW unified_datasets AS\n" + "\nUNION ALL\n".join(selects) + ";"
    
    # Check if view exists before creating
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.views 
            WHERE table_schema = 'public' 
            AND table_name = 'unified_datasets'
        );
    """)
    view_existed = cursor.fetchone()[0]
    
    # IMPORTANT:
    # Postgres does not allow CREATE OR REPLACE VIEW to change the existing view's column
    # layout (adding/reordering columns). Since we may evolve the view schema over time,
    # drop first to avoid errors like:
    #   "cannot change name of view column ..."
    if view_existed:
        cursor.execute("DROP VIEW IF EXISTS unified_datasets;")

    # Create the view
    cursor.execute(create_view_sql)
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM unified_datasets")
    total_rows = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT source, COUNT(*) as count 
        FROM unified_datasets 
        GROUP BY source 
        ORDER BY source
    """)
    rows_by_source = {row[0]: row[1] for row in cursor.fetchall()}
    
    if view_existed:
        logger.info(f"Successfully replaced unified_datasets view ({total_rows} rows)")
    else:
        logger.info(f"Successfully created unified_datasets view ({total_rows} rows)")
    
    return {
        "view_created": True,
        "total_rows": total_rows,
        "rows_by_source": rows_by_source,
        "dandi_table_exists": dandi_table_exists,
        "openneuro_table_exists": openneuro_table_exists,
        "crcns_table_exists": crcns_table_exists,
        "sparc_table_exists": sparc_table_exists,
        "neuro_table_exists": neuro_table_exists,
    }



# ---------------------------------------------------------------------------
# Paper reuse classification schema (shared by the four *_paper_mapping DAGs
# and paper_reuse_classification)
# ---------------------------------------------------------------------------

# Per-source citation-classification tables, with their dataset id column.
PAPER_CITATION_CLASSIFICATION_TABLES = (
    ("dandi_paper_citation_classifications", "dandi_id"),
    ("openneuro_paper_citation_classifications", "openneuro_id"),
    ("crcns_paper_citation_classifications", "crcns_id"),
    ("sparc_paper_citation_classifications", "sparc_id"),
)

# Columns the whole-paper classifier (utils/classify_fulltext_reuse.py) writes
# beyond the original classification/confidence/reasoning/model/status set.
# ``same_lab``, ``same_lab_confidence`` and ``source_archive`` already exist on
# these tables and are reused. All nullable, all added with IF NOT EXISTS, so
# this is safe to run on every DAG start.
PAPER_CITATION_CLASSIFICATION_COLUMNS = (
    ("prompt_version", "INTEGER"),          # classify_fulltext_reuse.PROMPT_VERSION
    ("mode", "TEXT"),                       # 'citing' | 'direct'
    ("reuse_type", "TEXT"),                 # one of REUSE_TYPES, REUSE rows only
    ("reuse_type_other", "TEXT"),           # write-in when reuse_type = 'OTHER'
    ("reused_modalities", "JSONB"),         # list of MODALITIES
    ("reused_dandi_hosted", "BOOLEAN"),
    ("reused_neurophysiology", "BOOLEAN"),
    ("evidence_quotes", "JSONB"),           # verified quote records
    ("source_quotes", "JSONB"),
    ("quote_warnings", "JSONB"),
    ("hallucinated_quote_count", "INTEGER"),
    ("error_kind", "TEXT"),                 # set when status = 'error'
    ("usage", "JSONB"),                     # raw provider usage / cost
    ("input_chars", "INTEGER"),
    ("truncation", "JSONB"),
    ("provider", "TEXT"),
)

# Full-text provenance recorded by paper-text-fetcher (utils/paper_fulltext.py).
# Kept apart from the legacy fulltext_* columns, whose source vocabulary differs.
PAPERS_FULLTEXT_FETCHER_COLUMNS = (
    ("text_status", "TEXT"),                # 'full_text' | 'metadata_only' | 'unavailable'
    ("fulltext_fetcher_cache_key", "TEXT"),
    ("fulltext_fetcher_source", "TEXT"),
    ("fulltext_fetcher_fetched_at", "TIMESTAMPTZ"),
)

PAPER_REUSE_CLASSIFICATION_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS paper_reuse_classification_runs (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    dag_run_id TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ DEFAULT NOW(),
    model TEXT,
    prompt_version INTEGER,
    classification_scope TEXT,
    params JSONB,
    summary JSONB
);
"""


PAPERS_TEXT_STATUS_BACKFILL_SQL = """
UPDATE papers
SET text_status = CASE
        WHEN fulltext_available THEN 'full_text'
        WHEN fulltext_reason LIKE 'metadata_only:%' THEN 'metadata_only'
        WHEN fulltext_reason LIKE 'unavailable:%' THEN 'unavailable'
    END
WHERE text_status IS NULL
  AND (fulltext_available
       OR fulltext_reason LIKE 'metadata_only:%'
       OR fulltext_reason LIKE 'unavailable:%');
"""


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL;", (f"public.{table_name}",))
    row = cursor.fetchone()
    return bool(row and row[0])


def ensure_paper_reuse_classification_columns(cursor) -> Dict[str, Any]:
    """
    Add the whole-paper classification columns and the runs table.

    Idempotent: every statement is ``IF NOT EXISTS``. Tables that do not exist
    yet (a source whose mapping DAG has never run) are skipped; their own DAG
    creates them and calls this again. Returns which tables were touched.
    """
    touched: List[str] = []

    for table, _id_col in PAPER_CITATION_CLASSIFICATION_TABLES:
        if not _table_exists(cursor, table):
            continue
        for column, col_type in PAPER_CITATION_CLASSIFICATION_COLUMNS:
            cursor.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type};"
            )
        touched.append(table)

    if _table_exists(cursor, "papers"):
        for column, col_type in PAPERS_FULLTEXT_FETCHER_COLUMNS:
            cursor.execute(
                f"ALTER TABLE papers ADD COLUMN IF NOT EXISTS {column} {col_type};"
            )
        # Backfill text_status from what the mapping DAGs already recorded.
        # fulltext_available means the legacy path got an article body; the
        # "metadata_only: ..." / "unavailable: ..." prefixes are written into
        # fulltext_reason by utils.paper_fulltext.fetch_fulltext_oa once it
        # delegates to paper-text-fetcher. Legacy "no_oa_fulltext_found" rows
        # stay NULL: the wider fetcher may still find them.
        cursor.execute(PAPERS_TEXT_STATUS_BACKFILL_SQL)
        touched.append("papers")

    cursor.execute(PAPER_REUSE_CLASSIFICATION_RUNS_DDL)
    touched.append("paper_reuse_classification_runs")

    import logging
    logging.getLogger(__name__).info(
        "ensure_paper_reuse_classification_columns touched: %s", touched)
    return {"tables": touched}
