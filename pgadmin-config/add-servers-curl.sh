#!/bin/bash
# Add servers to pgAdmin using curl (no Python required)

PGADMIN_URL="http://localhost:5050"
EMAIL="admin@admin.com"
PASSWORD="admin"

echo "Waiting for pgAdmin to be ready..."
sleep 5

# Wait for pgAdmin
for i in {1..30}; do
    if curl -s -f "${PGADMIN_URL}/misc/ping" > /dev/null 2>&1; then
        echo "pgAdmin is ready!"
        break
    fi
    sleep 1
done

# Get CSRF token and login
echo "Logging in..."
COOKIE_JAR="/tmp/pgadmin_cookies.txt"
LOGIN_RESPONSE=$(curl -s -c "${COOKIE_JAR}" -b "${COOKIE_JAR}" \
    -X POST "${PGADMIN_URL}/authenticate/login" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "email=${EMAIL}&password=${PASSWORD}")

if echo "${LOGIN_RESPONSE}" | grep -q "access_token"; then
    echo "Login successful!"
    
    # Get server group ID (usually 1)
    SERVER_GROUP_ID=1
    
    # Add Airflow server
    echo "Adding 'Local PostgreSQL - Airflow' server..."
    curl -s -b "${COOKIE_JAR}" -X POST "${PGADMIN_URL}/browser/server/obj/" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "Local PostgreSQL - Airflow",
            "host": "postgres",
            "port": 5432,
            "maintenance_db": "airflow",
            "username": "airflow",
            "password": "airflow",
            "ssl_mode": "prefer",
            "comment": "Airflow metadata database",
            "servergroup": 1
        }' && echo " ✓ Added"
    
    # Add DAG Data server
    echo "Adding 'Local PostgreSQL - DAG Data' server..."
    curl -s -b "${COOKIE_JAR}" -X POST "${PGADMIN_URL}/browser/server/obj/" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "Local PostgreSQL - DAG Data",
            "host": "postgres",
            "port": 5432,
            "maintenance_db": "dag_data",
            "username": "airflow",
            "password": "airflow",
            "ssl_mode": "prefer",
            "comment": "DAG data storage database",
            "servergroup": 1
        }' && echo " ✓ Added"
    
    echo ""
    echo "Servers added! Please refresh your pgAdmin browser."
else
    echo "Login failed. You may need to add servers manually."
fi

rm -f "${COOKIE_JAR}"



