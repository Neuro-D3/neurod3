#!/bin/bash
# Register Caddy routes for a PR preview environment
# Usage: ./register-routes.sh <PR_NUMBER>
# Example: ./register-routes.sh 123

set -e

PR_NUMBER=$1
CADDY_API_URL=${CADDY_API_URL:-http://localhost:2019}

if [ -z "$PR_NUMBER" ]; then
    echo "Usage: $0 <PR_NUMBER>"
    echo "Example: $0 123"
    exit 1
fi

echo "Registering routes for PR #${PR_NUMBER}..."
echo "Caddy API URL: $CADDY_API_URL"

# Check if Caddy API is accessible
echo "Checking if Caddy API is accessible..."
if ! curl -fsS --max-time 2 "${CADDY_API_URL}/config/" > /dev/null 2>&1; then
    echo "✗ Caddy API is not accessible at ${CADDY_API_URL}"
    echo "  Ensure Caddy is running and the admin API is bound to 0.0.0.0:2019"
    exit 1
fi

echo "✓ Caddy API is accessible"

# Connect Caddy to PR network so it can reach services by name
PROJECT_NAME="pr-${PR_NUMBER}"
NETWORK_NAME="${PROJECT_NAME}-pr-network"
CADDY_CONTAINER="caddy-proxy"

echo "Connecting Caddy to PR network: ${NETWORK_NAME}..."
if docker network inspect "${NETWORK_NAME}" &>/dev/null; then
    if ! docker network inspect "${NETWORK_NAME}" | grep -q "\"${CADDY_CONTAINER}\""; then
        docker network connect "${NETWORK_NAME}" "${CADDY_CONTAINER}" 2>/dev/null && \
            echo "✓ Connected Caddy to ${NETWORK_NAME}" || \
            echo "⚠ Could not connect Caddy to network (may already be connected)"
    else
        echo "✓ Caddy is already connected to ${NETWORK_NAME}"
    fi
else
    echo "⚠ Network ${NETWORK_NAME} not found - services may not be running yet"
fi

# Function to ensure HTTP app exists
ensure_http_app() {
    echo "Ensuring HTTP app exists..."
    
    # Use PUT to set the HTTP app config (creates if doesn't exist, updates if exists)
    HTTP_CODE=$(curl -X PUT "${CADDY_API_URL}/config/apps/http" \
        -H "Content-Type: application/json" \
        -d "{}" \
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)
    
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ HTTP app ensured"
        return 0
    elif [ "$HTTP_CODE" = "409" ]; then
        # HTTP 409 means it already exists, which is fine
        echo "✓ HTTP app already exists"
        return 0
    else
        ERROR_RESPONSE=$(curl -X PUT "${CADDY_API_URL}/config/apps/http" \
            -H "Content-Type: application/json" \
            -d "{}" \
            -s --max-time 5 2>&1)
        echo "✗ Failed to ensure HTTP app (HTTP ${HTTP_CODE})"
        echo "  Response: $ERROR_RESPONSE"
        return 1
    fi
}

# Function to ensure server srv0 exists
ensure_server() {
    local server_name="srv0"
    echo "Ensuring server ${server_name} exists..."
    
    # Use PUT to set the server config
    local server_config='{"listen": [":80"]}'
    HTTP_CODE=$(curl -X PUT "${CADDY_API_URL}/config/apps/http/servers/${server_name}" \
        -H "Content-Type: application/json" \
        -d "${server_config}" \
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)
    
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ Server ${server_name} ensured"
        return 0
    elif [ "$HTTP_CODE" = "409" ]; then
        # HTTP 409 means it already exists, which is fine
        echo "✓ Server ${server_name} already exists"
        return 0
    else
        ERROR_RESPONSE=$(curl -X PUT "${CADDY_API_URL}/config/apps/http/servers/${server_name}" \
            -H "Content-Type: application/json" \
            -d "${server_config}" \
            -s --max-time 5 2>&1)
        echo "✗ Failed to ensure server ${server_name} (HTTP ${HTTP_CODE})"
        echo "  Response: $ERROR_RESPONSE"
        return 1
    fi
}

# Initialize HTTP app and server structure
ensure_http_app || exit 1
ensure_server || exit 1

# Function to clear all routes for this PR (idempotency)
clear_pr_routes() {
    echo "Clearing existing routes for PR #${PR_NUMBER}..."
    
    # Get all existing routes
    EXISTING_ROUTES_JSON=$(curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" 2>/dev/null)
    
    # Handle null/empty response
    if [ -z "$EXISTING_ROUTES_JSON" ] || [ "$EXISTING_ROUTES_JSON" = "null" ]; then
        echo "  No existing routes to clear"
        return 0
    fi
    
    # Filter out routes matching this PR's pattern (pr-${PR_NUMBER}-*)
    if command -v jq &> /dev/null; then
        FILTERED_ROUTES=$(echo "$EXISTING_ROUTES_JSON" | jq "[.[] | select(.\"@id\" | startswith(\"pr-${PR_NUMBER}-\") | not)]" 2>/dev/null)
        
        if [ -z "$FILTERED_ROUTES" ] || [ "$FILTERED_ROUTES" = "null" ]; then
            FILTERED_ROUTES="[]"
        fi
        
        ROUTES_TO_REMOVE=$(echo "$EXISTING_ROUTES_JSON" | jq "[.[] | select(.\"@id\" | startswith(\"pr-${PR_NUMBER}-\"))] | length" 2>/dev/null || echo "0")
        
        if [ "$ROUTES_TO_REMOVE" = "0" ]; then
            echo "  No PR routes to clear"
            return 0
        fi
        
        echo "  Removing ${ROUTES_TO_REMOVE} route(s) for PR #${PR_NUMBER}"
        
        # PATCH to replace existing routes (works when routes key already exists)
        # Fallback to PUT if routes key doesn't exist (404)
        HTTP_CODE=$(curl -X PATCH "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
            -H "Content-Type: application/json" \
            -d "${FILTERED_ROUTES}" \
            -s -o /dev/null -w "%{http_code}" \
            --max-time 5 \
            --connect-timeout 2 2>&1)
        
        # If PATCH returns 404, routes key doesn't exist yet, use PUT to create it
        if [ "$HTTP_CODE" = "404" ]; then
            echo "  Debug: Routes key doesn't exist, using PUT to create..."
            HTTP_CODE=$(curl -X PUT "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
                -H "Content-Type: application/json" \
                -d "${FILTERED_ROUTES}" \
                -s -o /dev/null -w "%{http_code}" \
                --max-time 5 \
                --connect-timeout 2 2>&1)
        fi
        
        if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
            echo "✓ Cleared PR routes"
            return 0
        else
            echo "⚠ Failed to clear PR routes (HTTP ${HTTP_CODE}), but continuing..."
            ERROR_RESPONSE=$(curl -X PATCH "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
                -H "Content-Type: application/json" \
                -d "${FILTERED_ROUTES}" \
                -s --max-time 5 2>&1 | head -3)
            if [ -n "$ERROR_RESPONSE" ]; then
                echo "  Error: $ERROR_RESPONSE"
            fi
            return 0  # Don't fail, just continue
        fi
    else
        echo "⚠ jq not available, cannot filter routes. Continuing anyway..."
        return 0
    fi
}

# Clear existing routes for this PR before registering new ones
clear_pr_routes

# Function to delete a route by @id (for idempotency)
delete_route() {
    local route_id=$1
    
    # Try deleting by route ID using @id path
    HTTP_CODE=$(curl -X DELETE "${CADDY_API_URL}/config/apps/http/servers/srv0/routes/@id/${route_id}" \
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)
    
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "  Deleted existing route: ${route_id}"
        return 0
    elif [ "$HTTP_CODE" = "404" ]; then
        # Route doesn't exist, that's fine
        return 0
    else
        # Try alternative delete path (without @id)
        HTTP_CODE=$(curl -X DELETE "${CADDY_API_URL}/config/apps/http/servers/srv0/routes/${route_id}" \
            -s -o /dev/null -w "%{http_code}" \
            --max-time 5 \
            --connect-timeout 2 2>&1)
        
        if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
            echo "  Deleted existing route: ${route_id}"
            return 0
        elif [ "$HTTP_CODE" = "404" ]; then
            # Route doesn't exist, that's fine
            return 0
        else
            # Ignore other errors during deletion - we'll try to add anyway
            return 0
        fi
    fi
}

# Function to register a route (idempotent: deletes by @id before adding)
register_route() {
    local service_name=$1
    local path_prefix=$2
    local upstream_host=$3
    local upstream_port=$4
    local route_id="pr-${PR_NUMBER}-${service_name}"
    
    echo "Registering route: ${path_prefix}* -> ${upstream_host}:${upstream_port}"
    echo "  Route ID: ${route_id}"
    
    # Debug: Check current routes before deletion
    echo "  Debug: Checking existing routes..."
    EXISTING_ROUTES=$(curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" 2>/dev/null || echo "[]")
    ROUTE_COUNT=$(echo "$EXISTING_ROUTES" | jq 'length' 2>/dev/null || echo "0")
    echo "  Debug: Found ${ROUTE_COUNT} existing route(s)"
    
    # Delete existing route by @id for idempotency
    delete_route "${route_id}"
    
    # Debug: Check routes after deletion
    EXISTING_ROUTES_AFTER=$(curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" 2>/dev/null || echo "[]")
    ROUTE_COUNT_AFTER=$(echo "$EXISTING_ROUTES_AFTER" | jq 'length' 2>/dev/null || echo "0")
    echo "  Debug: Routes after deletion: ${ROUTE_COUNT_AFTER}"
    
    # Create route configuration JSON with strip_path_prefix
    local route_config=$(cat <<EOF
{
    "@id": "${route_id}",
    "match": [
        {
            "path": ["${path_prefix}*"]
        }
    ],
    "handle": [
        {
            "handler": "rewrite",
            "strip_path_prefix": "${path_prefix}"
        },
        {
            "handler": "reverse_proxy",
            "upstreams": [
                {
                    "dial": "${upstream_host}:${upstream_port}"
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
    
    # Debug: Show what we're about to POST
    echo "  Debug: Route config to POST:"
    echo "${route_config}" | jq . 2>/dev/null || echo "${route_config}"
    
    # After clearing PR routes, ensure routes array exists (not null), then POST array
    # Caddy requires routes to be an array, not null
    echo "  Debug: Ensuring routes array exists before POSTing..."
    
    # Check if routes is null (doesn't exist) vs empty array []
    CURRENT_ROUTES=$(curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" 2>/dev/null || echo "null")
    
    if [ "$CURRENT_ROUTES" = "null" ] || [ -z "$CURRENT_ROUTES" ]; then
        echo "  Debug: Routes is null, initializing with empty array..."
        # Initialize routes as empty array first
        INIT_CODE=$(curl -X PUT "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
            -H "Content-Type: application/json" \
            -d "[]" \
            -s -o /dev/null -w "%{http_code}" \
            --max-time 5 \
            --connect-timeout 2 2>&1)
        echo "  Debug: Routes initialization code: ${INIT_CODE}"
    fi
    
    # POST route as an array (Caddy API expects RouteList = array)
    # Wrap the route object in an array
    local route_array=$(cat <<EOF
[${route_config}]
EOF
)
    
    echo "  Debug: POSTing route as array..."
    HTTP_CODE=$(curl -X POST "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
        -H "Content-Type: application/json" \
        -d "${route_array}" \
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)
    
    echo "  Debug: POST response code: ${HTTP_CODE}"
    
    if [ "$HTTP_CODE" = "000" ] || [ -z "$HTTP_CODE" ]; then
        echo "✗ Failed to register route: ${path_prefix} (Connection failed)"
        return 1
    elif [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ Route registered: ${path_prefix}*"
        return 0
    else
        echo "✗ Failed to register route: ${path_prefix} (HTTP ${HTTP_CODE})"
        ERROR_MSG=$(curl -X POST "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
            -H "Content-Type: application/json" \
            -d "${route_array}" \
            -s --max-time 5 2>&1 | head -10)
        if [ -n "$ERROR_MSG" ]; then
            echo "  Error: $ERROR_MSG"
        fi
        echo "  Debug: Current routes after failure:"
        curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" | jq . 2>/dev/null || curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes"
        return 1
    fi
}

# Service configurations: service_name, path_prefix, upstream_host, upstream_port
# Use Docker service names from docker-compose.pr.yml
# Network name is prefixed with project name: pr-<NUMBER>-pr-network
# Service names are: airflow-webserver, frontend, api, pgadmin

# Airflow (service: airflow-webserver, port: 8080)
register_route "airflow" "/pr-${PR_NUMBER}/airflow" "airflow-webserver" "8080"
register_route "airflow-slash" "/pr-${PR_NUMBER}/airflow/" "airflow-webserver" "8080"

# Frontend App (service: frontend, port: 3000)
register_route "app" "/pr-${PR_NUMBER}/app" "frontend" "3000"
register_route "app-slash" "/pr-${PR_NUMBER}/app/" "frontend" "3000"

# API (service: api, port: 8000)
register_route "api" "/pr-${PR_NUMBER}/api" "api" "8000"
register_route "api-slash" "/pr-${PR_NUMBER}/api/" "api" "8000"

# pgAdmin (service: pgadmin, port: 80 - internal port)
register_route "pgadmin" "/pr-${PR_NUMBER}/pgadmin" "pgadmin" "80"
register_route "pgadmin-slash" "/pr-${PR_NUMBER}/pgadmin/" "pgadmin" "80"

echo ""
echo "✓ All routes registered successfully!"
echo "  Routes are configured for path-based access:"
echo "  - Airflow: /pr-${PR_NUMBER}/airflow"
echo "  - App: /pr-${PR_NUMBER}/app"
echo "  - API: /pr-${PR_NUMBER}/api"
echo "  - pgAdmin: /pr-${PR_NUMBER}/pgadmin"
echo ""
echo "  Access these via: http://<PUBLIC_IP>/pr-${PR_NUMBER}/<service>"

exit 0

