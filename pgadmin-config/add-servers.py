#!/usr/bin/env python3
"""
Script to add servers to pgAdmin via REST API.
This works even if pgAdmin has already been initialized.
"""
import requests
import json
import time
import sys

# pgAdmin configuration
PGADMIN_URL = "http://localhost:5050"
PGADMIN_EMAIL = "admin@admin.com"
PGADMIN_PASSWORD = "admin"

# Server configurations
SERVERS = [
    {
        "name": "Local PostgreSQL - Airflow",
        "host": "postgres",
        "port": 5432,
        "maintenance_db": "airflow",
        "username": "airflow",
        "password": "airflow",
        "ssl_mode": "prefer",
        "comment": "Airflow metadata database"
    },
    {
        "name": "Local PostgreSQL - DAG Data",
        "host": "postgres",
        "port": 5432,
        "maintenance_db": "dag_data",
        "username": "airflow",
        "password": "airflow",
        "ssl_mode": "prefer",
        "comment": "DAG data storage database"
    }
]

def wait_for_pgadmin(max_retries=30):
    """Wait for pgAdmin to be ready."""
    print("Waiting for pgAdmin to be ready...")
    for i in range(max_retries):
        try:
            response = requests.get(f"{PGADMIN_URL}/misc/ping", timeout=2)
            if response.status_code == 200:
                print("pgAdmin is ready!")
                return True
        except:
            pass
        time.sleep(1)
    return False

def login():
    """Login to pgAdmin and get session token."""
    print("Logging in to pgAdmin...")
    session = requests.Session()
    
    # Get CSRF token
    response = session.get(f"{PGADMIN_URL}/browser/")
    if response.status_code != 200:
        print(f"Failed to access pgAdmin: {response.status_code}")
        return None, None
    
    # Login
    login_data = {
        "email": PGADMIN_EMAIL,
        "password": PGADMIN_PASSWORD
    }
    
    response = session.post(
        f"{PGADMIN_URL}/authenticate/login",
        data=login_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    
    if response.status_code != 200:
        print(f"Login failed: {response.status_code}")
        print(response.text)
        return None, None
    
    print("Login successful!")
    return session, response.json().get("access_token")

def get_server_groups(session):
    """Get server groups."""
    response = session.get(f"{PGADMIN_URL}/browser/server_group/obj/")
    if response.status_code == 200:
        groups = response.json()
        if groups:
            return groups[0].get("id", 1)
    return 1  # Default server group ID

def get_existing_servers(session, server_group_id):
    """Get list of existing servers."""
    response = session.get(f"{PGADMIN_URL}/browser/server_group/children/{server_group_id}/")
    if response.status_code == 200:
        return [s.get("id") for s in response.json()]
    return []

def add_server(session, server_group_id, server_config):
    """Add a server to pgAdmin."""
    server_data = {
        "name": server_config["name"],
        "host": server_config["host"],
        "port": server_config["port"],
        "maintenance_db": server_config["maintenance_db"],
        "username": server_config["username"],
        "password": server_config["password"],
        "ssl_mode": server_config["ssl_mode"],
        "comment": server_config.get("comment", ""),
        "servergroup": server_group_id
    }
    
    response = session.post(
        f"{PGADMIN_URL}/browser/server/obj/",
        json=server_data,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code in [200, 201]:
        print(f"✓ Added server: {server_config['name']}")
        return True
    else:
        print(f"✗ Failed to add server {server_config['name']}: {response.status_code}")
        print(f"  Response: {response.text}")
        return False

def main():
    """Main function."""
    if not wait_for_pgadmin():
        print("pgAdmin is not ready. Please wait and try again.")
        sys.exit(1)
    
    session, token = login()
    if not session:
        print("Failed to login to pgAdmin.")
        sys.exit(1)
    
    server_group_id = get_server_groups(session)
    existing_servers = get_existing_servers(session, server_group_id)
    
    print(f"\nFound {len(existing_servers)} existing server(s)")
    print("Adding servers...\n")
    
    success_count = 0
    for server in SERVERS:
        # Check if server already exists (by name)
        # For simplicity, we'll try to add it anyway (pgAdmin will handle duplicates)
        if add_server(session, server_group_id, server):
            success_count += 1
    
    print(f"\n✓ Successfully added {success_count}/{len(SERVERS)} server(s)")
    print("\nPlease refresh your pgAdmin browser to see the new servers!")

if __name__ == "__main__":
    main()



