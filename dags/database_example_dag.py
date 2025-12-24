"""
Example DAG demonstrating database operations with environment detection.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.environment import is_local_environment, get_database_config
from utils.database import (
    create_table_if_not_exists,
    execute_query,
    execute_update,
    get_db_connection
)


def detect_environment(**context):
    """Detect if running locally or hosted."""
    is_local = is_local_environment()
    config = get_database_config()
    
    print(f"Environment: {'LOCAL' if is_local else 'HOSTED'}")
    print(f"Database Config: {config}")
    
    if is_local:
        print("Running in LOCAL mode - using local PostgreSQL database")
        # You can add local-specific logic here
    else:
        print("Running in HOSTED mode - using hosted database")
        # You can add hosted-specific logic here
    
    return {'is_local': is_local, 'config': config}


def setup_database(**context):
    """Create tables if they don't exist."""
    # Create a sample table for storing data
    create_table_if_not_exists(
        'dag_execution_log',
        """
        id SERIAL PRIMARY KEY,
        dag_id VARCHAR(255) NOT NULL,
        task_id VARCHAR(255) NOT NULL,
        execution_date TIMESTAMP NOT NULL,
        status VARCHAR(50) NOT NULL,
        message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """
    )
    
    # Create another example table
    create_table_if_not_exists(
        'sample_data',
        """
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        value NUMERIC(10, 2),
        category VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """
    )
    
    print("Database tables created/verified successfully")
    return "Tables ready"


def insert_sample_data(**context):
    """Insert sample data into the database."""
    # Get execution date - use logical_date (newer Airflow) or execution_date (older)
    # The database utility will handle Proxy conversion automatically
    execution_date = context.get('logical_date') or context.get('data_interval_start') or context.get('execution_date') or datetime.now()
    dag_id = context['dag'].dag_id
    
    # Insert execution log
    # Note: execute_update will automatically convert Proxy objects to actual values
    query = """
    INSERT INTO dag_execution_log (dag_id, task_id, execution_date, status, message)
    VALUES (%(dag_id)s, %(task_id)s, %(execution_date)s, %(status)s, %(message)s)
    """
    
    execute_update(
        query,
        {
            'dag_id': dag_id,
            'task_id': 'insert_sample_data',
            'execution_date': execution_date,
            'status': 'running',
            'message': f'DAG {dag_id} executed at {execution_date}'
        }
    )
    
    # Insert sample data
    sample_query = """
    INSERT INTO sample_data (name, value, category)
    VALUES 
        (%(name1)s, %(value1)s, %(category1)s),
        (%(name2)s, %(value2)s, %(category2)s),
        (%(name3)s, %(value3)s, %(category3)s)
    """
    
    execute_update(
        sample_query,
        {
            'name1': 'Product A', 'value1': 100.50, 'category1': 'Electronics',
            'name2': 'Product B', 'value2': 250.75, 'category2': 'Clothing',
            'name3': 'Product C', 'value3': 50.25, 'category3': 'Food'
        }
    )
    
    print("Sample data inserted successfully")
    return "Data inserted"


def query_data(**context):
    """Query data from the database."""
    # Query execution logs
    logs = execute_query(
        "SELECT * FROM dag_execution_log ORDER BY created_at DESC LIMIT 10"
    )
    
    print(f"Found {len(logs)} execution logs")
    for log in logs:
        print(f"  - {log}")
    
    # Query sample data
    samples = execute_query(
        "SELECT * FROM sample_data ORDER BY created_at DESC LIMIT 10"
    )
    
    print(f"Found {len(samples)} sample records")
    for sample in samples:
        print(f"  - {sample}")
    
    return {'logs_count': len(logs), 'samples_count': len(samples)}


def update_data(**context):
    """Update data in the database."""
    # Update the status of the latest log entry
    update_query = """
    UPDATE dag_execution_log 
    SET status = %(status)s, message = %(message)s
    WHERE id = (SELECT MAX(id) FROM dag_execution_log)
    """
    
    rows_updated = execute_update(
        update_query,
        {
            'status': 'completed',
            'message': 'DAG execution completed successfully'
        }
    )
    
    print(f"Updated {rows_updated} row(s)")
    return f"Updated {rows_updated} rows"


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'database_example_dag',
    default_args=default_args,
    description='Example DAG demonstrating database operations with environment detection',
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['database', 'example'],
) as dag:
    
    # Task 1: Detect environment
    detect_env = PythonOperator(
        task_id='detect_environment',
        python_callable=detect_environment,
    )
    
    # Task 2: Setup database tables
    setup_db = PythonOperator(
        task_id='setup_database',
        python_callable=setup_database,
    )
    
    # Task 3: Insert sample data
    insert_data = PythonOperator(
        task_id='insert_sample_data',
        python_callable=insert_sample_data,
    )
    
    # Task 4: Query data
    query_data_task = PythonOperator(
        task_id='query_data',
        python_callable=query_data,
    )
    
    # Task 5: Update data
    update_data_task = PythonOperator(
        task_id='update_data',
        python_callable=update_data,
    )
    
    # Define task dependencies
    detect_env >> setup_db >> insert_data >> query_data_task >> update_data_task

