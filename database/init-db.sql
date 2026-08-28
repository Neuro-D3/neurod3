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
    -- Legacy field (no longer maintained). Keep nullable for backward compatibility.
    citations INTEGER,
    -- Number of associated papers (nullable by default; populated for DANDI over time).
    papers INTEGER,
    url TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, dataset_id)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_datasets_source ON neuroscience_datasets(source);
CREATE INDEX IF NOT EXISTS idx_datasets_modality ON neuroscience_datasets(modality);
CREATE INDEX IF NOT EXISTS idx_datasets_papers ON neuroscience_datasets(papers DESC);

-- Data-source registry: producer DAGs upsert one row per (source_name, contract_name)
-- after validating their produced tables against the data contracts in
-- airflow/dags/contracts/. Consumers (unified_datasets view, API, classifier) iterate
-- this registry instead of hardcoding per-source table names.
-- Canonical definition: update this file AND utils/contracts.ensure_registry_table
-- if the schema changes. The runtime function creates this table idempotently on
-- first DAG run, so existing databases self-heal without re-running this script.
CREATE TABLE IF NOT EXISTS data_sources (
    source_name   TEXT NOT NULL,
    contract_name TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    source_id_col TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_name, contract_name)
);

-- Grant permissions to airflow user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

