#!/bin/bash
# Register Caddy routes for a PR preview environment
# Usage: ./register-routes.sh <PR_NUMBER> <DOMAIN>
# Example: ./register-routes.sh 123 preview.example.com

set +e  # Don't exit on error, we'll handle errors manually

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
    HTTP_CODE=$(curl -X POST "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
        -H "Content-Type: application/json" \
        -d "${route_config}" \
        -s -o /dev/null -w "%{http_code}")
    
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ Route added: ${hostname}"
        return 0
    else
        echo "✗ Failed to add route: ${hostname} (HTTP ${HTTP_CODE})"
        # Try to get error message
        curl -X POST "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
            -H "Content-Type: application/json" \
            -d "${route_config}" \
            -s 2>&1 | head -5
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
CADDY_CONTAINER=""
if docker ps --format '{{.Names}}' | grep -q "^caddy-proxy$"; then
    CADDY_CONTAINER="caddy-proxy"
elif docker ps --format '{{.Names}}' | grep -q "^caddy$"; then
    CADDY_CONTAINER="caddy"
fi

if ! docker network inspect "${NETWORK_NAME}" &>/dev/null; then
    echo "Warning: Network ${NETWORK_NAME} not found"
    echo "This might be okay if containers are on the default network"
else
    # Connect Caddy container to PR network if it exists
    if [ -n "$CADDY_CONTAINER" ]; then
        if docker network inspect "${NETWORK_NAME}" | grep -q "\"${CADDY_CONTAINER}\""; then
            echo "✓ Caddy is already connected to ${NETWORK_NAME}"
        else
            docker network connect "${NETWORK_NAME}" "${CADDY_CONTAINER}" 2>/dev/null && \
                echo "✓ Connected Caddy to ${NETWORK_NAME}" || \
                echo "⚠ Could not connect Caddy to network (may already be connected)"
        fi
    else
        echo "⚠ Caddy container not found, routes may not work"
    fi
fi

# Add routes (continue even if some fail)
ROUTES_ADDED=0
ROUTES_FAILED=0

if add_route "pr-${PR_NUMBER}.airflow.${DOMAIN}" "${AIRFLOW_CONTAINER}" "8080"; then
    ROUTES_ADDED=$((ROUTES_ADDED + 1))
else
    ROUTES_FAILED=$((ROUTES_FAILED + 1))
fi

if add_route "pr-${PR_NUMBER}.app.${DOMAIN}" "${FRONTEND_CONTAINER}" "3000"; then
    ROUTES_ADDED=$((ROUTES_ADDED + 1))
else
    ROUTES_FAILED=$((ROUTES_FAILED + 1))
fi

if add_route "pr-${PR_NUMBER}.pgadmin.${DOMAIN}" "${PGADMIN_CONTAINER}" "80"; then
    ROUTES_ADDED=$((ROUTES_ADDED + 1))
else
    ROUTES_FAILED=$((ROUTES_FAILED + 1))
fi

if add_route "pr-${PR_NUMBER}.api.${DOMAIN}" "${API_CONTAINER}" "8000"; then
    ROUTES_ADDED=$((ROUTES_ADDED + 1))
else
    ROUTES_FAILED=$((ROUTES_FAILED + 1))
fi

echo ""
echo "Routes summary: ${ROUTES_ADDED} added, ${ROUTES_FAILED} failed"

# Reload Caddy configuration
echo "Reloading Caddy configuration..."
HTTP_CODE=$(curl -X POST "${CADDY_API_URL}/reload" -s -o /dev/null -w "%{http_code}")

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
    echo "✓ Caddy configuration reloaded"
    echo ""
    if [ $ROUTES_ADDED -gt 0 ]; then
        echo "Routes registered successfully!"
        echo "  - Airflow: https://pr-${PR_NUMBER}.airflow.${DOMAIN}"
        echo "  - App: https://pr-${PR_NUMBER}.app.${DOMAIN}"
        echo "  - API: https://pr-${PR_NUMBER}.api.${DOMAIN}"
        echo "  - pgAdmin: https://pr-${PR_NUMBER}.pgadmin.${DOMAIN}"
    fi
    if [ $ROUTES_FAILED -gt 0 ]; then
        echo "⚠ Some routes failed to register, but deployment will continue"
    fi
    exit 0
else
    echo "⚠ Failed to reload Caddy configuration (HTTP ${HTTP_CODE})"
    echo "Routes may still work, but Caddy may need manual reload"
    # Don't exit with error, just warn
    exit 0
fi

