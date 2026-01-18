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
    echo "  Ensure Caddy is running and the admin API is reachable at ${CADDY_API_URL} (on Docker setups, bind 2019 to 127.0.0.1 on the host)"
    exit 1
fi

echo "✓ Caddy API is accessible"

# Connect Caddy to PR network so it can reach services by name
PROJECT_NAME="pr-${PR_NUMBER}"

# Compose usually creates <project>_<network> (underscore). Some setups use hyphen.
NETWORK_NAME="$(docker network ls --format '{{.Name}}' | grep -E "^${PROJECT_NAME}([-_])pr-network$" | head -n1 || true)"
CADDY_CONTAINER="caddy-proxy"

echo "Connecting Caddy to PR network: ${NETWORK_NAME:-<not found>}..."

if [ -z "$NETWORK_NAME" ]; then
  echo "⚠ Could not find PR network for ${PROJECT_NAME}. Expected ${PROJECT_NAME}_pr-network or ${PROJECT_NAME}-pr-network"
else
  if docker network inspect "${NETWORK_NAME}" | grep -q "\"${CADDY_CONTAINER}\""; then
    echo "✓ Caddy is already connected to ${NETWORK_NAME}"
  else
    docker network connect "${NETWORK_NAME}" "${CADDY_CONTAINER}" 2>/dev/null && \
      echo "✓ Connected Caddy to ${NETWORK_NAME}" || \
      echo "⚠ Could not connect Caddy to network (may already be connected)"
  fi
fi

# Require the HTTP app/server to exist.
# We intentionally do NOT try to create/overwrite `apps/http` here because Caddy's base config
# should define the server (see `deploy/caddy/Caddyfile`), and overwriting can wipe routes.
echo "Checking that Caddy HTTP app/server exist..."
if ! curl -fsS --max-time 2 "${CADDY_API_URL}/config/apps/http" > /dev/null 2>&1; then
  echo "::error::Caddy HTTP app is missing. This usually means Caddy started without any :80 site block."
  echo "          Ensure your Caddyfile defines a :80 server (no catch-all 404), then restart Caddy."
  echo "          Current /config/ is:"
  curl -s "${CADDY_API_URL}/config/" || true
  exit 1
fi

if ! curl -fsS --max-time 2 "${CADDY_API_URL}/config/apps/http/servers/srv0" > /dev/null 2>&1; then
  echo "::error::Caddy HTTP server srv0 is missing. Restart Caddy, then re-run this script."
  exit 1
fi

echo "✓ Caddy HTTP app/server present"

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

make_route_no_strip() {
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

make_path_proxy_route() {
    local route_id="$1"
    local paths_json="$2"      # JSON array string, e.g. ["\/static\/js\/*","\/favicon.ico"]
    local upstream_host="$3"
    local upstream_port="$4"

    cat <<EOF
{
  "@id": "${route_id}",
  "match": [
    {
      "path": ${paths_json}
    }
  ],
  "handle": [
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

    # Remove any existing routes for this PR.
    # Some routes (e.g. base Caddyfile stub routes) may not have an @id; treat those as empty string.
    FILTERED_ROUTES=$(echo "$EXISTING_ROUTES_JSON" | jq "[.[] | select(((.\"@id\" // \"\") | startswith(\"pr-${PR_NUMBER}-\")) | not)]")

    # Desired routes for this PR
    DESIRED_ROUTES=$(cat <<EOF
[
$(make_path_proxy_route "pr-${PR_NUMBER}-frontend-static" "[\"/static/js/*\",\"/static/css/*\",\"/static/media/*\",\"/asset-manifest.json\",\"/manifest.json\",\"/favicon.ico\",\"/robots.txt\"]" "frontend" "3000"),
$(make_path_proxy_route "pr-${PR_NUMBER}-airflow-static" "[\"/static/assets/*\",\"/static/pin_*.png\"]" "airflow-webserver" "8080"),
$(make_path_proxy_route "pr-${PR_NUMBER}-airflow-api-root" "[\"/api/*\"]" "airflow-webserver" "8080"),
$(make_path_proxy_route "pr-${PR_NUMBER}-airflow-ui-root" "[\"/auth*\",\"/auth/*\",\"/login*\",\"/logout*\",\"/home*\",\"/dags*\",\"/ui/*\"]" "airflow-webserver" "8080"),
$(make_route "pr-${PR_NUMBER}-airflow" "/pr-${PR_NUMBER}/airflow" "airflow-webserver" "8080"),
$(make_route "pr-${PR_NUMBER}-airflow-slash" "/pr-${PR_NUMBER}/airflow/" "airflow-webserver" "8080"),
$(make_route "pr-${PR_NUMBER}-app" "/pr-${PR_NUMBER}/app" "frontend" "3000"),
$(make_route "pr-${PR_NUMBER}-app-slash" "/pr-${PR_NUMBER}/app/" "frontend" "3000"),
$(make_route "pr-${PR_NUMBER}-api" "/pr-${PR_NUMBER}/api" "api" "8000"),
$(make_route "pr-${PR_NUMBER}-api-slash" "/pr-${PR_NUMBER}/api/" "api" "8000"),
$(make_route_no_strip "pr-${PR_NUMBER}-pgadmin" "/pr-${PR_NUMBER}/pgadmin" "pgadmin" "80"),
$(make_route_no_strip "pr-${PR_NUMBER}-pgadmin-slash" "/pr-${PR_NUMBER}/pgadmin/" "pgadmin" "80")
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

