#!/bin/bash
# Script to add servers to pgAdmin using Python script
# This works even if pgAdmin has already been initialized

echo "Waiting for pgAdmin to be ready..."
sleep 20

# Install requests if not available
python3 -c "import requests" 2>/dev/null || pip3 install requests --quiet

# Run the Python script
python3 /pgadmin-config/add-servers.py



