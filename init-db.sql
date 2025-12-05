-- Create a separate database for storing DAG data
-- This script runs automatically when the PostgreSQL container is first initialized
CREATE DATABASE dag_data;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE dag_data TO airflow;

-- Connect to dag_data database and create schema
\c dag_data;

-- Create neuroscience_datasets table
CREATE TABLE IF NOT EXISTS neuroscience_datasets (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    dataset_id VARCHAR(255) NOT NULL,
    title TEXT NOT NULL,
    modality VARCHAR(100) NOT NULL,
    citations INTEGER DEFAULT 0,
    url TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, dataset_id)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_datasets_source ON neuroscience_datasets(source);
CREATE INDEX IF NOT EXISTS idx_datasets_modality ON neuroscience_datasets(modality);
CREATE INDEX IF NOT EXISTS idx_datasets_citations ON neuroscience_datasets(citations DESC);

-- Grant permissions to airflow user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

