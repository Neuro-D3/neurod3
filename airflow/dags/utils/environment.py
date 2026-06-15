"""
Utility functions to detect if Airflow is running locally or in a hosted environment.
This is a utility module, not a DAG file.
"""
# Prevent this file from being treated as a DAG
# Airflow will skip files that don't define a 'dag' object

import os
from typing import Optional


def is_local_environment() -> bool:
    """
    Detect if Airflow is running in a local environment.
    
    Returns:
        bool: True if running locally, False if hosted
    """
    # Check for common environment variables that indicate hosted environments
    hosted_indicators = [
        'AIRFLOW__CORE__EXECUTOR',  # If set to KubernetesExecutor, likely hosted
        'KUBERNETES_SERVICE_HOST',  # Kubernetes environment
        'ECS_CONTAINER_METADATA_URI',  # AWS ECS
        'GOOGLE_CLOUD_PROJECT',  # GCP
        'AZURE_CLOUD',  # Azure
    ]
    
    # If any hosted indicator is present, assume hosted
    for indicator in hosted_indicators:
        if os.environ.get(indicator):
            # Special case: LocalExecutor is typically local
            if indicator == 'AIRFLOW__CORE__EXECUTOR':
                executor = os.environ.get(indicator, '').lower()
                if 'local' in executor:
                    return True
            else:
                return False
    
    # Check if running in Docker (local development)
    # If we're in Docker but no hosted indicators, assume local
    if os.path.exists('/.dockerenv'):
        return True
    
    # Default to local if no indicators found
    return True


def get_database_config() -> dict:
    """
    Get database configuration for the application data DB (`dag_data`).

    Switched by EXPLICIT config, not environment auto-detection: if DB_HOST is set
    (staging/prod — e.g. the GCE compose points it at the Cloud SQL proxy), the
    config is built from the DB_* env vars. Only when DB_HOST is unset do we fall
    back to the local-dev defaults (the bundled `postgres` container in the root
    docker-compose.yml).

    NOTE: do NOT key this off is_local_environment() / LocalExecutor / /.dockerenv.
    The staging VM also runs LocalExecutor inside Docker, so those falsely read as
    "local" and would send tasks to a non-existent `postgres` host.

    Returns:
        dict: Database connection parameters
    """
    db_host = os.environ.get('DB_HOST')

    if db_host:
        # Hosted (staging/prod): explicit coordinates from the environment.
        return {
            'host': db_host,
            'port': int(os.environ.get('DB_PORT', 5432)),
            'database': os.environ.get('DB_NAME', 'dag_data'),
            'user': os.environ.get('DB_USER', 'airflow'),
            # Empty (not "airflow") if unset, so a missing secret fails loudly at
            # connect time rather than silently trying the dev password.
            'password': os.environ.get('DB_PASSWORD', ''),
            'schema': os.environ.get('DB_SCHEMA', 'public'),
        }

    # Local development: the bundled Postgres from the root docker-compose.yml.
    return {
        'host': 'postgres',
        'port': 5432,
        'database': 'dag_data',
        'user': 'airflow',
        'password': 'airflow',
        'schema': 'public',
    }


def get_database_connection_string() -> str:
    """
    Get a SQLAlchemy connection string for the database.
    
    Returns:
        str: Connection string
    """
    config = get_database_config()
    return (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )

