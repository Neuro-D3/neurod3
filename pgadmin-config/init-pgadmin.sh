#!/bin/bash
# Initialize pgAdmin with pre-configured servers
# This script waits for pgAdmin to initialize, then copies the server configuration

PGADMIN_DIR="/var/lib/pgadmin"
USER_EMAIL="admin@admin.com"
SERVERS_DIR="${PGADMIN_DIR}/storage/${USER_EMAIL}"
SERVERS_FILE="${SERVERS_DIR}/servers.json"
CONFIG_FILE="/pgadmin-config/servers.json"

# Wait for pgAdmin directory structure to be created
echo "Waiting for pgAdmin to initialize..."
for i in {1..30}; do
    if [ -d "${PGADMIN_DIR}" ]; then
        echo "pgAdmin directory found"
        break
    fi
    sleep 1
done

# Create the storage directory for the user if it doesn't exist
mkdir -p "${SERVERS_DIR}"

# Wait a bit more for pgAdmin to fully initialize
sleep 3

# Copy the server configuration
# Always overwrite if PGADMIN_REPLACE_SERVERS_ON_STARTUP is true, or if servers.json doesn't exist
if [ -f "${CONFIG_FILE}" ]; then
    if [ "${PGADMIN_REPLACE_SERVERS_ON_STARTUP:-False}" = "True" ] || [ "${FORCE_OVERWRITE:-false}" = "true" ] || [ ! -f "${SERVERS_FILE}" ]; then
        echo "Installing server configuration..."
        cp "${CONFIG_FILE}" "${SERVERS_FILE}"
        chmod 600 "${SERVERS_FILE}"
        chown pgadmin:pgadmin "${SERVERS_FILE}" 2>/dev/null || true
        echo "Server configuration installed successfully"
    else
        echo "Server configuration already exists. Set FORCE_OVERWRITE=true to overwrite."
    fi
else
    echo "Warning: Configuration file not found at ${CONFIG_FILE}"
fi

