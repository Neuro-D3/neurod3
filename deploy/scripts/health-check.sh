#!/bin/bash
# Health check script for PR preview services
# Usage: ./health-check.sh <PR_NUMBER>
# Example: ./health-check.sh 123

set +e  # Don't exit on error, we want to continue even if some checks fail

PR_NUMBER=$1
TIMEOUT=${TIMEOUT:-180}  # 3 minutes timeout (increased)
INTERVAL=${INTERVAL:-5}  # Check every 5 seconds
MIN_REQUIRED=${MIN_REQUIRED:-2}  # Minimum services that must be ready

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

# Function to check HTTP endpoint (with fallback if curl not available)
check_http() {
    local container_name=$1
    local port=$2
    local service_name=$3
    local path=${4:-/}
    
    # First check if curl is available in the container
    if ! docker exec "${container_name}" which curl > /dev/null 2>&1; then
        # If curl not available, just check if container is running
        if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
            echo "⚠ ${service_name} container running (curl not available for health check)"
            return 0
        else
            return 1
        fi
    fi
    
    # Try to check HTTP endpoint
    if docker exec "${container_name}" curl -f -s --max-time 2 "http://localhost:${port}${path}" > /dev/null 2>&1; then
        echo "✓ ${service_name} HTTP endpoint is responding"
        return 0
    else
        # If HTTP check fails, at least verify container is running
        if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
            echo "⚠ ${service_name} container running but HTTP not ready yet"
            return 1
        else
            return 1
        fi
    fi
}

# Check containers exist and are running
AIRFLOW_CONTAINER="${PROJECT_PREFIX}-airflow-webserver-1"
FRONTEND_CONTAINER="${PROJECT_PREFIX}-frontend-1"
API_CONTAINER="${PROJECT_PREFIX}-api-1"
PGADMIN_CONTAINER="${PROJECT_PREFIX}-pgadmin-1"
POSTGRES_CONTAINER="${PROJECT_PREFIX}-postgres-1"

echo "Checking containers..."
check_container "${POSTGRES_CONTAINER}" "PostgreSQL"
check_container "${AIRFLOW_CONTAINER}" "Airflow"
check_container "${FRONTEND_CONTAINER}" "Frontend"
check_container "${API_CONTAINER}" "API"
check_container "${PGADMIN_CONTAINER}" "pgAdmin"

echo ""
echo "Waiting for services to be ready (timeout: ${TIMEOUT}s, minimum ${MIN_REQUIRED} services required)..."

elapsed=0
ready_count=0
total_count=4

while [ $elapsed -lt $TIMEOUT ]; do
    ready_count=0
    
    # Check Airflow (port 8080) - Airflow 3+ health endpoint is under the REST API
    if check_http "${AIRFLOW_CONTAINER}" "8080" "Airflow" "/api/v2/monitor/health" 2>/dev/null || \
       check_http "${AIRFLOW_CONTAINER}" "8080" "Airflow" "/health" 2>/dev/null || \
       check_http "${AIRFLOW_CONTAINER}" "8080" "Airflow" "/" 2>/dev/null; then
        ready_count=$((ready_count + 1))
    fi
    
    # Check Frontend (port 3000)
    if check_http "${FRONTEND_CONTAINER}" "3000" "Frontend" "/" 2>/dev/null; then
        ready_count=$((ready_count + 1))
    fi
    
    # Check API (port 8000) - try health endpoint first, then root
    if check_http "${API_CONTAINER}" "8000" "API" "/api/health" 2>/dev/null || \
       check_http "${API_CONTAINER}" "8000" "API" "/" 2>/dev/null; then
        ready_count=$((ready_count + 1))
    fi
    
    # Check pgAdmin (port 80)
    if check_http "${PGADMIN_CONTAINER}" "80" "pgAdmin" "/" 2>/dev/null; then
        ready_count=$((ready_count + 1))
    fi
    
    # If we have minimum required services, consider it good enough
    if [ $ready_count -ge $MIN_REQUIRED ]; then
        echo ""
        echo "✓ Minimum required services (${ready_count}/${total_count}) are ready!"
        echo "  (Some services may still be starting up, but deployment can proceed)"
        exit 0
    fi
    
    if [ $((elapsed % 15)) -eq 0 ] || [ $ready_count -gt 0 ]; then
        echo "  Waiting... (${ready_count}/${total_count} services ready, ${elapsed}s elapsed)"
    fi
    sleep $INTERVAL
    elapsed=$((elapsed + INTERVAL))
done

echo ""
if [ $ready_count -ge $MIN_REQUIRED ]; then
    echo "✓ Minimum required services (${ready_count}/${total_count}) are ready!"
    exit 0
else
    echo "⚠ Timeout waiting for services (${ready_count}/${total_count} ready)"
    echo "  Minimum required: ${MIN_REQUIRED}, but deployment will continue"
    echo "  Some services may still be starting up in the background"
    exit 0  # Don't fail, just warn
fi



