-- Create a separate database for storing DAG data
-- This script runs automatically when the PostgreSQL container is first initialized
CREATE DATABASE dag_data;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE dag_data TO airflow;

