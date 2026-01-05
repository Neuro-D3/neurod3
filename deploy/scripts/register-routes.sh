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

echo "Caddy API URL: $CADDY_API_URL"

# Check if Caddy is accessible
echo "Checking if Caddy API is accessible..."
if curl -s --max-time 2 "${CADDY_API_URL}/config/" > /dev/null 2>&1; then
    echo "✓ Caddy API is accessible"
else
    echo "✗ Caddy API is not accessible at ${CADDY_API_URL}"
    echo "Checking if Caddy container is running..."
    
    # Check for Caddy containers
    if docker ps --format '{{.Names}}' | grep -qE "^caddy|^caddy-proxy"; then
        CADDY_CONTAINER=$(docker ps --format '{{.Names}}' | grep -E "^caddy|^caddy-proxy" | head -n1)
        echo "Found Caddy container: $CADDY_CONTAINER"
        echo "Container status:"
        docker ps --filter "name=$CADDY_CONTAINER" --format "  {{.Names}}: {{.Status}}"
        
        # Try to get Caddy API from container
        echo "Attempting to access Caddy API from container..."
        if docker exec "$CADDY_CONTAINER" curl -s --max-time 2 "http://localhost:2019/config/" > /dev/null 2>&1; then
            echo "✓ Caddy API is accessible from inside the container"
            echo "⚠ Caddy API is not accessible from the host. Routes will need to be configured manually or Caddy needs to expose the admin API."
        else
            echo "✗ Caddy API is not accessible even from inside the container"
            echo "Caddy may not be configured with admin API enabled"
        fi
    else
        echo "✗ No Caddy container found"
        echo "Available containers:"
        docker ps --format "  {{.Names}}" | head -10
    fi
    
    echo ""
    echo "⚠ Continuing without Caddy route registration..."
    echo "You may need to manually configure Caddy routes or ensure Caddy is running and accessible"
    exit 0  # Don't fail, just warn
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
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)
    
    # Check if curl failed to connect (HTTP 000)
    if [ "$HTTP_CODE" = "000" ] || [ -z "$HTTP_CODE" ]; then
        echo "✗ Failed to add route: ${hostname} (Connection failed - Caddy API not reachable)"
        echo "  Check if Caddy is running and the API URL is correct: ${CADDY_API_URL}"
        return 1
    elif [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ Route added: ${hostname}"
        return 0
    else
        echo "✗ Failed to add route: ${hostname} (HTTP ${HTTP_CODE})"
        # Try to get error message
        ERROR_MSG=$(curl -X POST "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
            -H "Content-Type: application/json" \
            -d "${route_config}" \
            -s --max-time 5 2>&1 | head -3)
        if [ -n "$ERROR_MSG" ]; then
            echo "  Error: $ERROR_MSG"
        fi
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

# Reload Caddy configuration (only if we added at least one route)
if [ $ROUTES_ADDED -gt 0 ]; then
    echo "Reloading Caddy configuration..."
    HTTP_CODE=$(curl -X POST "${CADDY_API_URL}/reload" \
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)
    
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ Caddy configuration reloaded"
    elif [ "$HTTP_CODE" = "000" ] || [ -z "$HTTP_CODE" ]; then
        echo "⚠ Could not reload Caddy (API not reachable)"
    else
        echo "⚠ Failed to reload Caddy configuration (HTTP ${HTTP_CODE})"
    fi
fi

echo ""
if [ $ROUTES_ADDED -gt 0 ]; then
    echo "✓ Routes registered successfully!"
    echo "  - Airflow: https://pr-${PR_NUMBER}.airflow.${DOMAIN}"
    echo "  - App: https://pr-${PR_NUMBER}.app.${DOMAIN}"
    echo "  - API: https://pr-${PR_NUMBER}.api.${DOMAIN}"
    echo "  - pgAdmin: https://pr-${PR_NUMBER}.pgadmin.${DOMAIN}"
elif [ $ROUTES_FAILED -gt 0 ]; then
    echo "⚠ All routes failed to register"
    echo "  Caddy API may not be accessible or Caddy may not be running"
    echo "  You may need to manually configure routes or start Caddy"
fi

# Always exit successfully - route registration failure shouldn't block deployment
exit 0

