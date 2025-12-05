# Manual pgAdmin Server Setup (Quick & Easy)

Since automated setup is complex, here's the simple manual method (takes ~1 minute):

## Steps:

1. **Open pgAdmin**: http://localhost:5050
   - Email: `admin@admin.com`
   - Password: `admin`

2. **Add First Server (Airflow)**:
   - Right-click **"Servers"** in the left sidebar
   - Select **"Register"** → **"Server"**
   - **General Tab**:
     - Name: `Local PostgreSQL - Airflow`
   - **Connection Tab**:
     - Host name/address: `postgres` ⚠️ (NOT localhost!)
     - Port: `5432`
     - Maintenance database: `airflow`
     - Username: `airflow`
     - Password: `airflow`
   - Click **"Save"**

3. **Add Second Server (DAG Data)**:
   - Right-click **"Servers"** again
   - Select **"Register"** → **"Server"**
   - **General Tab**:
     - Name: `Local PostgreSQL - DAG Data`
   - **Connection Tab**:
     - Host name/address: `postgres` ⚠️ (NOT localhost!)
     - Port: `5432`
     - Maintenance database: `dag_data`
     - Username: `airflow`
     - Password: `airflow`
   - Click **"Save"**

## Important Notes:

- ⚠️ **Use `postgres` as the hostname**, not `localhost` - this is the Docker service name
- The servers will be saved permanently - you only need to do this once
- After adding, you can expand the servers to see your databases and tables

## That's it!

The servers are now configured and will persist across container restarts.



