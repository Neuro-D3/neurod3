#!/bin/bash
# Script to create the dag_data database if it doesn't exist
# This is useful if the database was already initialized before init-db.sql was added

docker-compose exec -T postgres psql -U airflow -d postgres <<EOF
SELECT 'CREATE DATABASE dag_data'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dag_data')\gexec

GRANT ALL PRIVILEGES ON DATABASE dag_data TO airflow;
EOF

echo "Database 'dag_data' created or already exists"

