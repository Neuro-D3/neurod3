#!/bin/bash
# Register Caddy routes for a PR preview environment
# Usage: ./register-routes.sh <PR_NUMBER> <DOMAIN>
# Example: ./register-routes.sh 123 preview.example.com

set -e

PR_NUMBER=$1
DOMAIN=$2
CADDY_API_URL=${CADDY_API_URL:-http://localhost:2019}

if [ -z "$PR_NUMBER" ] || [ -z "$DOMAIN" ]; then
    echo "Usage: $0 <PR_NUMBER> <DOMAIN>"
    echo "Example: $0 123 preview.example.com"
    exit 1
fi

# Container names based on Docker Compose project naming
# Docker Compose uses format: <project>-<service>-<index>
PROJECT_PREFIX="pr-${PR_NUMBER}"
AIRFLOW_CONTAINER="${PROJECT_PREFIX}-airflow-webserver-1"
FRONTEND_CONTAINER="${PROJECT_PREFIX}-frontend-1"
PGADMIN_CONTAINER="${PROJECT_PREFIX}-pgadmin-1"

# Network name
NETWORK_NAME="${PROJECT_PREFIX}-pr-network"

echo "Registering routes for PR #${PR_NUMBER}..."

# Function to add a route via Caddy admin API
add_route() {
    local hostname=$1
    local upstream=$2
    local port=$3
    
    echo "Adding route: ${hostname} -> ${upstream}:${port}"
    
    # Create route configuration JSON
    local route_config=$(cat <<EOF
{
    "@id": "pr-${PR_NUMBER}-${hostname}",
    "match": [
        {
            "host": ["${hostname}"]
        }
    ],
    "handle": [
        {
            "handler": "reverse_proxy",
            "upstreams": [
                {
                    "dial": "${upstream}:${port}"
                }
            ],
            "transport": {
                "protocol": "http"
            }
        }
    ],
    "terminal": true
}
EOF
)
    
    # Add route to Caddy
    curl -X POST "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
        -H "Content-Type: application/json" \
        -d "${route_config}" \
        -s -o /dev/null
    
    if [ $? -eq 0 ]; then
        echo "✓ Route added: ${hostname}"
    else
        echo "✗ Failed to add route: ${hostname}"
        return 1
    fi
}

# Wait for containers to be ready
echo "Waiting for containers to be ready..."
sleep 5

# Container names
API_CONTAINER="${PROJECT_PREFIX}-api-1"

# Check if containers exist
if ! docker ps --format '{{.Names}}' | grep -q "^${AIRFLOW_CONTAINER}$"; then
    echo "Warning: Airflow container ${AIRFLOW_CONTAINER} not found"
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${FRONTEND_CONTAINER}$"; then
    echo "Warning: Frontend container ${FRONTEND_CONTAINER} not found"
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${API_CONTAINER}$"; then
    echo "Warning: API container ${API_CONTAINER} not found"
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${PGADMIN_CONTAINER}$"; then
    echo "Warning: pgAdmin container ${PGADMIN_CONTAINER} not found"
fi

# Connect Caddy to the PR network if not already connected
if ! docker network inspect "${NETWORK_NAME}" &>/dev/null; then
    echo "Warning: Network ${NETWORK_NAME} not found"
else
    # Connect Caddy container to PR network
    if docker ps --format '{{.Names}}' | grep -q "^caddy-proxy$"; then
        docker network connect "${NETWORK_NAME}" caddy-proxy 2>/dev/null || true
        echo "✓ Connected Caddy to ${NETWORK_NAME}"
    fi
fi

# Add routes
add_route "pr-${PR_NUMBER}.airflow.${DOMAIN}" "${AIRFLOW_CONTAINER}" "8080"
add_route "pr-${PR_NUMBER}.app.${DOMAIN}" "${FRONTEND_CONTAINER}" "3000"
add_route "pr-${PR_NUMBER}.pgadmin.${DOMAIN}" "${PGADMIN_CONTAINER}" "80"
add_route "pr-${PR_NUMBER}.api.${DOMAIN}" "${API_CONTAINER}" "8000"

# Reload Caddy configuration
echo "Reloading Caddy configuration..."
curl -X POST "${CADDY_API_URL}/reload" -s -o /dev/null

if [ $? -eq 0 ]; then
    echo "✓ Caddy configuration reloaded"
    echo ""
    echo "Routes registered successfully!"
    echo "  - Airflow: https://pr-${PR_NUMBER}.airflow.${DOMAIN}"
    echo "  - App: https://pr-${PR_NUMBER}.app.${DOMAIN}"
    echo "  - API: https://pr-${PR_NUMBER}.api.${DOMAIN}"
    echo "  - pgAdmin: https://pr-${PR_NUMBER}.pgadmin.${DOMAIN}"
else
    echo "✗ Failed to reload Caddy configuration"
    exit 1
fi

