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

# Ensure jq is available (required for Option 1 route filtering)
if ! command -v jq &>/dev/null; then
    echo "::error::jq is required by deploy/scripts/register-routes.sh (Option 1) but was not found on PATH"
    exit 1
fi

make_route() {
    local route_id="$1"
    local path_prefix="$2"
    local upstream_host="$3"
    local upstream_port="$4"

    cat <<EOF
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
}

apply_pr_routes() {
    echo "Applying routes for PR #${PR_NUMBER} (Option 1: clear PR routes, keep others)..."

    EXISTING_ROUTES_JSON=$(curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" 2>/dev/null || echo "null")
    if [ -z "$EXISTING_ROUTES_JSON" ] || [ "$EXISTING_ROUTES_JSON" = "null" ]; then
        EXISTING_ROUTES_JSON="[]"
    fi

    # Remove any existing routes for this PR
    FILTERED_ROUTES=$(echo "$EXISTING_ROUTES_JSON" | jq "[.[] | select(.\"@id\" | startswith(\"pr-${PR_NUMBER}-\") | not)]")

    # Desired routes for this PR
    DESIRED_ROUTES=$(cat <<EOF
[
$(make_route "pr-${PR_NUMBER}-airflow" "/pr-${PR_NUMBER}/airflow" "airflow-webserver" "8080"),
$(make_route "pr-${PR_NUMBER}-airflow-slash" "/pr-${PR_NUMBER}/airflow/" "airflow-webserver" "8080"),
$(make_route "pr-${PR_NUMBER}-app" "/pr-${PR_NUMBER}/app" "frontend" "3000"),
$(make_route "pr-${PR_NUMBER}-app-slash" "/pr-${PR_NUMBER}/app/" "frontend" "3000"),
$(make_route "pr-${PR_NUMBER}-api" "/pr-${PR_NUMBER}/api" "api" "8000"),
$(make_route "pr-${PR_NUMBER}-api-slash" "/pr-${PR_NUMBER}/api/" "api" "8000"),
$(make_route "pr-${PR_NUMBER}-pgadmin" "/pr-${PR_NUMBER}/pgadmin" "pgadmin" "80"),
$(make_route "pr-${PR_NUMBER}-pgadmin-slash" "/pr-${PR_NUMBER}/pgadmin/" "pgadmin" "80")
]
EOF
)

    NEW_ROUTES=$(echo "$FILTERED_ROUTES" | jq --argjson desired "$DESIRED_ROUTES" '. + $desired')

    echo "  Debug: routes before: $(echo "$EXISTING_ROUTES_JSON" | jq 'length')"
    echo "  Debug: routes after:  $(echo "$NEW_ROUTES" | jq 'length')"

    # Use PATCH to replace existing routes value (works when key exists)
    HTTP_CODE=$(curl -X PATCH "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
        -H "Content-Type: application/json" \
        -d "${NEW_ROUTES}" \
        -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        --connect-timeout 2 2>&1)

    # If routes key doesn't exist yet, PATCH returns 404. Create with PUT.
    if [ "$HTTP_CODE" = "404" ]; then
        HTTP_CODE=$(curl -X PUT "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" \
            -H "Content-Type: application/json" \
            -d "${NEW_ROUTES}" \
            -s -o /dev/null -w "%{http_code}" \
            --max-time 5 \
            --connect-timeout 2 2>&1)
    fi

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "✓ Routes applied for PR #${PR_NUMBER}"
        return 0
    fi

    echo "::error::Failed to apply routes (HTTP ${HTTP_CODE})"
    curl -s "${CADDY_API_URL}/config/apps/http/servers/srv0/routes" | jq . || true
    return 1
}

apply_pr_routes || exit 1

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

