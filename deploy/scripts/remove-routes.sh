#!/bin/bash
# Remove Caddy routes for a PR preview environment
# Usage: ./remove-routes.sh <PR_NUMBER>
# Example: ./remove-routes.sh 123

set +e  # Don't exit on error, we'll handle errors manually

PR_NUMBER=$1
CADDY_API_URL=${CADDY_API_URL:-http://localhost:2019}

if [ -z "$PR_NUMBER" ]; then
    echo "Usage: $0 <PR_NUMBER>"
    echo "Example: $0 123"
    exit 1
fi

echo "Removing routes for PR #${PR_NUMBER}..."

# Function to remove a route via Caddy admin API
remove_route() {
    local route_id=$1
    local path=$2
    
    echo "Removing route: ${path} (ID: ${route_id})"
    
    # Delete route by ID
    HTTP_CODE=$(curl -X DELETE "${CADDY_API_URL}/config/apps/http/servers/srv0/routes/${route_id}" \
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)
    
    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ Route removed: ${path}"
        return 0
    elif [ "$HTTP_CODE" = "404" ]; then
        echo "⚠ Route not found: ${path} (may have already been removed)"
        return 0
    else
        echo "✗ Failed to remove route: ${path} (HTTP ${HTTP_CODE})"
        return 1
    fi
}

# Get all routes and find matching ones by path pattern
routes_json=$(curl -s --max-time 5 "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" 2>/dev/null)

if [ -z "$routes_json" ] || [ "$routes_json" = "null" ]; then
    echo "⚠ Could not retrieve routes from Caddy API"
    echo "Routes may have already been removed or Caddy is not accessible"
    exit 0
fi

# Remove routes matching our PR pattern by path
# Route IDs are: pr-<NUMBER>-airflow, pr-<NUMBER>-app, pr-<NUMBER>-api, pr-<NUMBER>-pgadmin
for service in "airflow" "app" "api" "pgadmin"; do
    route_id="pr-${PR_NUMBER}-${service}"
    path="/pr-${PR_NUMBER}/${service}"
    
    # Try to find route by ID first
    route_exists=$(echo "$routes_json" | jq -r ".[] | select(.\"@id\" == \"${route_id}\") | .\"@id\"" 2>/dev/null | head -1)
    
    if [ -n "$route_exists" ] && [ "$route_exists" != "null" ]; then
        remove_route "$route_id" "$path"
    else
        # Fallback: try to find by path pattern
        route_id_by_path=$(echo "$routes_json" | jq -r ".[] | select(.match[0].path[0] == \"${path}/*\") | .\"@id\"" 2>/dev/null | head -1)
        if [ -n "$route_id_by_path" ] && [ "$route_id_by_path" != "null" ]; then
            remove_route "$route_id_by_path" "$path"
        else
            echo "Route not found: ${path}"
        fi
    fi
done

# Reload Caddy configuration
echo "Reloading Caddy configuration..."
HTTP_CODE=$(curl -X POST "${CADDY_API_URL}/reload" \
    -s -o /dev/null -w "%{http_code}" \
    --max-time 5 \
    --connect-timeout 2 2>&1)

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
    echo "✓ Caddy configuration reloaded"
    echo "Routes removed successfully!"
elif [ "$HTTP_CODE" = "000" ] || [ -z "$HTTP_CODE" ]; then
    echo "⚠ Could not reload Caddy (API not reachable)"
    echo "Routes may have been removed, but Caddy needs manual reload"
else
    echo "⚠ Failed to reload Caddy configuration (HTTP ${HTTP_CODE})"
fi

# Always exit successfully - route removal failure shouldn't block teardown
exit 0

# Disconnect Caddy from PR network (optional cleanup)
NETWORK_NAME="pr-${PR_NUMBER}-pr-network"
if docker network inspect "${NETWORK_NAME}" &>/dev/null; then
    if docker ps --format '{{.Names}}' | grep -q "^caddy-proxy$"; then
        docker network disconnect "${NETWORK_NAME}" caddy-proxy 2>/dev/null || true
        echo "✓ Disconnected Caddy from ${NETWORK_NAME}"
    fi
fi

