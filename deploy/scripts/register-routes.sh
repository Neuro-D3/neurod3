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

# Function to register a route (replaces existing route by @id)
register_route() {
    local service_name=$1
    local path_prefix=$2
    local upstream_host=$3
    local upstream_port=$4
    local route_id="pr-${PR_NUMBER}-${service_name}"
    
    echo "Registering route: ${path_prefix}* -> ${upstream_host}:${upstream_port}"
    
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
    
    # Use PUT to replace existing route by @id
    HTTP_CODE=$(curl -X PUT "${CADDY_API_URL}/config/apps/http/servers/srv0/routes/${route_id}" \
        -H "Content-Type: application/json" \
        -d "${route_config}" \
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)
    
    if [ "$HTTP_CODE" = "000" ] || [ -z "$HTTP_CODE" ]; then
        echo "✗ Failed to register route: ${path_prefix} (Connection failed)"
        return 1
    elif [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ Route registered: ${path_prefix}*"
        return 0
    else
        # If PUT fails with 404, try POST (route doesn't exist yet)
        if [ "$HTTP_CODE" = "404" ]; then
            HTTP_CODE=$(curl -X POST "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
                -H "Content-Type: application/json" \
                -d "${route_config}" \
                -s -o /dev/null -w "%{http_code}" \
                --max-time 5 \
                --connect-timeout 2 2>&1)
            
            if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
                echo "✓ Route registered: ${path_prefix}*"
                return 0
            fi
        fi
        
        echo "✗ Failed to register route: ${path_prefix} (HTTP ${HTTP_CODE})"
        ERROR_MSG=$(curl -X PUT "${CADDY_API_URL}/config/apps/http/servers/srv0/routes/${route_id}" \
            -H "Content-Type: application/json" \
            -d "${route_config}" \
            -s --max-time 5 2>&1 | head -3)
        if [ -n "$ERROR_MSG" ]; then
            echo "  Error: $ERROR_MSG"
        fi
        return 1
    fi
}

# Service configurations: service_name, path_prefix, upstream_host, upstream_port
# Register routes for both /pr-<N>/service and /pr-<N>/service/ patterns

# Airflow
register_route "airflow" "/pr-${PR_NUMBER}/airflow" "host.docker.internal" "8080"
register_route "airflow-slash" "/pr-${PR_NUMBER}/airflow/" "host.docker.internal" "8080"

# Frontend App
register_route "app" "/pr-${PR_NUMBER}/app" "host.docker.internal" "3000"
register_route "app-slash" "/pr-${PR_NUMBER}/app/" "host.docker.internal" "3000"

# API
register_route "api" "/pr-${PR_NUMBER}/api" "host.docker.internal" "8000"
register_route "api-slash" "/pr-${PR_NUMBER}/api/" "host.docker.internal" "8000"

# pgAdmin (using port 5050 as standard host port)
register_route "pgadmin" "/pr-${PR_NUMBER}/pgadmin" "host.docker.internal" "5050"
register_route "pgadmin-slash" "/pr-${PR_NUMBER}/pgadmin/" "host.docker.internal" "5050"

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
