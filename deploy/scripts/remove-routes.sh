#!/bin/bash
# Remove Caddy routes for a PR preview environment
# Usage: ./remove-routes.sh <PR_NUMBER>
# Example: ./remove-routes.sh 123

set -e

PR_NUMBER=$1
CADDY_API_URL=${CADDY_API_URL:-http://localhost:2019}
DOMAIN=${DOMAIN:-preview.example.com}

if [ -z "$PR_NUMBER" ]; then
    echo "Usage: $0 <PR_NUMBER>"
    echo "Example: $0 123"
    exit 1
fi

echo "Removing routes for PR #${PR_NUMBER}..."

# Function to remove a route via Caddy admin API
remove_route() {
    local route_id=$1
    local hostname=$2
    
    echo "Removing route: ${hostname} (ID: ${route_id})"
    
    # Get current routes
    local routes=$(curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes")
    
    # Find and remove the route
    # Note: This is a simplified approach. In production, you might want to
    # use a more robust method to find and delete routes by ID.
    
    # Delete route by ID
    curl -X DELETE "${CADDY_API_URL}/config/apps/http/servers/srv0/routes/${route_id}" \
        -s -o /dev/null
    
    if [ $? -eq 0 ]; then
        echo "✓ Route removed: ${hostname}"
    else
        echo "✗ Failed to remove route: ${hostname} (may not exist)"
    fi
}

# Remove routes by finding them first
# Get all routes and find matching ones
routes_json=$(curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes")

# Remove routes matching our PR pattern
for hostname in "pr-${PR_NUMBER}.airflow.${DOMAIN}" "pr-${PR_NUMBER}.app.${DOMAIN}" "pr-${PR_NUMBER}.api.${DOMAIN}" "pr-${PR_NUMBER}.pgadmin.${DOMAIN}"; do
    route_id=$(echo "$routes_json" | jq -r ".[] | select(.match[0].host[0] == \"${hostname}\") | .@id" | head -1)
    if [ -n "$route_id" ] && [ "$route_id" != "null" ]; then
        remove_route "$route_id" "$hostname"
    else
        echo "Route not found: ${hostname}"
    fi
done

# Reload Caddy configuration
echo "Reloading Caddy configuration..."
curl -X POST "${CADDY_API_URL}/reload" -s -o /dev/null

if [ $? -eq 0 ]; then
    echo "✓ Caddy configuration reloaded"
    echo "Routes removed successfully!"
else
    echo "✗ Failed to reload Caddy configuration"
    exit 1
fi

# Disconnect Caddy from PR network (optional cleanup)
NETWORK_NAME="pr-${PR_NUMBER}-pr-network"
if docker network inspect "${NETWORK_NAME}" &>/dev/null; then
    if docker ps --format '{{.Names}}' | grep -q "^caddy-proxy$"; then
        docker network disconnect "${NETWORK_NAME}" caddy-proxy 2>/dev/null || true
        echo "✓ Disconnected Caddy from ${NETWORK_NAME}"
    fi
fi

