#!/bin/bash
# Health check script for PR preview services
# Usage: ./health-check.sh <PR_NUMBER>
# Example: ./health-check.sh 123

set -e

PR_NUMBER=$1
TIMEOUT=${TIMEOUT:-120}  # 2 minutes timeout
INTERVAL=${INTERVAL:-5}  # Check every 5 seconds

if [ -z "$PR_NUMBER" ]; then
    echo "Usage: $0 <PR_NUMBER>"
    echo "Example: $0 123"
    exit 1
fi

PROJECT_PREFIX="pr-${PR_NUMBER}"
NETWORK_NAME="${PROJECT_PREFIX}-pr-network"

echo "Checking health of PR #${PR_NUMBER} services..."

# Function to check if a container is healthy
check_container() {
    local container_name=$1
    local service_name=$2
    
    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        local status=$(docker inspect --format='{{.State.Status}}' "${container_name}" 2>/dev/null || echo "not-found")
        if [ "$status" = "running" ]; then
            echo "✓ ${service_name} container is running"
            return 0
        else
            echo "✗ ${service_name} container is not running (status: ${status})"
            return 1
        fi
    else
        echo "✗ ${service_name} container not found"
        return 1
    fi
}

# Function to check HTTP endpoint
check_http() {
    local container_name=$1
    local port=$2
    local service_name=$3
    
    # Use docker exec to check from inside the network
    if docker exec "${container_name}" curl -f -s "http://localhost:${port}" > /dev/null 2>&1; then
        echo "✓ ${service_name} HTTP endpoint is responding"
        return 0
    else
        echo "✗ ${service_name} HTTP endpoint is not responding"
        return 1
    fi
}

# Check containers exist and are running
AIRFLOW_CONTAINER="${PROJECT_PREFIX}-airflow-webserver-1"
FRONTEND_CONTAINER="${PROJECT_PREFIX}-frontend-1"
API_CONTAINER="${PROJECT_PREFIX}-api-1"
PGADMIN_CONTAINER="${PROJECT_PREFIX}-pgadmin-1"
POSTGRES_CONTAINER="${PROJECT_PREFIX}-postgres-1"

echo "Checking containers..."
check_container "${POSTGRES_CONTAINER}" "PostgreSQL" || exit 1
check_container "${AIRFLOW_CONTAINER}" "Airflow" || exit 1
check_container "${FRONTEND_CONTAINER}" "Frontend" || exit 1
check_container "${API_CONTAINER}" "API" || exit 1
check_container "${PGADMIN_CONTAINER}" "pgAdmin" || exit 1

echo ""
echo "Waiting for services to be ready (timeout: ${TIMEOUT}s)..."

elapsed=0
all_ready=false

while [ $elapsed -lt $TIMEOUT ]; do
    ready_count=0
    total_count=4
    
    # Check Airflow (port 8080)
    if docker exec "${AIRFLOW_CONTAINER}" curl -f -s "http://localhost:8080/health" > /dev/null 2>&1; then
        ready_count=$((ready_count + 1))
    fi
    
    # Check Frontend (port 3000)
    if docker exec "${FRONTEND_CONTAINER}" curl -f -s "http://localhost:3000" > /dev/null 2>&1; then
        ready_count=$((ready_count + 1))
    fi
    
    # Check API (port 8000)
    if docker exec "${API_CONTAINER}" curl -f -s "http://localhost:8000/api/health" > /dev/null 2>&1; then
        ready_count=$((ready_count + 1))
    fi
    
    # Check pgAdmin (port 80)
    if docker exec "${PGADMIN_CONTAINER}" curl -f -s "http://localhost:80" > /dev/null 2>&1; then
        ready_count=$((ready_count + 1))
    fi
    
    if [ $ready_count -eq $total_count ]; then
        all_ready=true
        break
    fi
    
    echo "  Waiting... (${ready_count}/${total_count} services ready, ${elapsed}s elapsed)"
    sleep $INTERVAL
    elapsed=$((elapsed + INTERVAL))
done

echo ""
if [ "$all_ready" = true ]; then
    echo "✓ All services are healthy and ready!"
    exit 0
else
    echo "✗ Timeout waiting for services to be ready"
    echo "  Some services may still be starting up"
    exit 1
fi


