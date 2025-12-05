#!/bin/bash
# Script to set up pgAdmin server configurations
# This runs inside the pgAdmin container

PGADMIN_DIR="/var/lib/pgadmin"
USER_EMAIL="admin@admin.com"
SERVERS_DIR="${PGADMIN_DIR}/storage/${USER_EMAIL}"
SERVERS_FILE="${SERVERS_DIR}/servers.json"

# Create directory if it doesn't exist
mkdir -p "${SERVERS_DIR}"

# Copy servers.json if it doesn't exist or if we want to overwrite
if [ ! -f "${SERVERS_FILE}" ] || [ "${OVERWRITE_SERVERS:-false}" = "true" ]; then
    if [ -f "/pgadmin-config/servers.json" ]; then
        cp /pgadmin-config/servers.json "${SERVERS_FILE}"
        echo "Server configuration copied to ${SERVERS_FILE}"
    fi
fi

# Set proper permissions
chown -R pgadmin:pgadmin "${SERVERS_DIR}"
chmod 600 "${SERVERS_FILE}" 2>/dev/null || true

echo "pgAdmin server configuration setup complete"



