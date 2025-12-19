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
    Create or replace the unified_datasets view that combines dandi_dataset and neuroscience_datasets.
    
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
            AND table_name = 'neuroscience_datasets'
        );
    """)
    neuro_table_exists = cursor.fetchone()[0]
    
    if not dandi_table_exists and not neuro_table_exists:
        logger.warning("Neither dandi_dataset nor neuroscience_datasets tables exist. Cannot create view.")
        return {
            "view_created": False,
            "total_rows": 0,
            "rows_by_source": {},
            "dandi_table_exists": False,
            "neuro_table_exists": False,
        }
    
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
        # Only dandi_dataset exists
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
        # Only neuroscience_datasets exists
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
    
    # Check if view exists before creating
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.views 
            WHERE table_schema = 'public' 
            AND table_name = 'unified_datasets'
        );
    """)
    view_existed = cursor.fetchone()[0]
    
    # Create or replace the view
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
        "neuro_table_exists": neuro_table_exists,
    }
