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
    Get database configuration based on environment.
    
    Returns:
        dict: Database connection parameters
    """
    is_local = is_local_environment()
    
    if is_local:
        # Local PostgreSQL configuration
        return {
            'host': 'postgres',
            'port': 5432,
            'database': 'dag_data',
            'user': 'airflow',
            'password': 'airflow',
            'schema': 'public'
        }
    else:
        # Hosted environment - you can customize this later
        # For now, return None or raise an error
        # You can add environment variables for hosted DB config
        return {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': int(os.environ.get('DB_PORT', 5432)),
            'database': os.environ.get('DB_NAME', 'dag_data'),
            'user': os.environ.get('DB_USER', 'airflow'),
            'password': os.environ.get('DB_PASSWORD', 'airflow'),
            'schema': os.environ.get('DB_SCHEMA', 'public')
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

