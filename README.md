# Apache Airflow Local Development Setup

This project provides a Docker-based setup for running Apache Airflow locally.

## Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- At least 4GB of RAM available for Docker
- At least 10GB of free disk space

## Quick Start

1. **Set the Airflow user ID** (Linux/Mac only):
   ```bash
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```
   
   On Windows, the `.env` file is already configured with `AIRFLOW_UID=50000`.

2. **Initialize Airflow** (first time only):
   ```bash
   docker-compose up airflow-init
   ```

3. **Start Airflow**:
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI**:
   - Open your browser and go to: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

5. **Access pgAdmin (Database Management)**:
   - Open your browser and go to: http://localhost:5050
   - Email: `admin@admin.com`
   - Password: `admin`
   - **Database servers are automatically configured!** You should see:
     - `Local PostgreSQL - Airflow` (airflow database)
     - `Local PostgreSQL - DAG Data` (dag_data database)
   - **First time connecting to a server:**
     - Click on a server name
     - Enter the password: `airflow`
     - Check "Save password" to avoid entering it again
   - If servers don't appear, try refreshing the browser (Ctrl+Shift+R or Cmd+Shift+R)
   - **Manual server addition** (if needed):
     - Right-click "Servers" → "Register" → "Server"
     - Name: `Local PostgreSQL - Airflow` or `Local PostgreSQL - DAG Data`
     - Host: `postgres` (important: use `postgres`, not `localhost`)
     - Port: `5432`
     - Database: `airflow` or `dag_data`
     - Username: `airflow`
     - Password: `airflow`

6. **Access Frontend Dashboard**:
   - Open your browser and go to: http://localhost:3000
   - The React frontend will display data from the PostgreSQL database
   - The frontend automatically starts with `docker-compose up -d`

7. **Stop Airflow**:
   ```bash
   docker-compose down
   ```

## Project Structure

```
.
├── docker-compose.yml    # Docker Compose configuration
├── Dockerfile            # Custom Airflow image with dependencies
├── requirements.txt      # Python package dependencies
├── .env                  # Environment variables
├── dags/                 # Your DAG files go here
├── logs/                 # Airflow logs (auto-created)
├── plugins/              # Custom Airflow plugins
├── config/               # Airflow configuration files
└── frontend/             # React + TypeScript frontend
    ├── src/              # Source code
    │   ├── types/        # TypeScript type definitions
    │   │   └── data.ts   # Data types (empty, ready for your types)
    │   ├── App.tsx       # Main React component
    │   └── index.tsx     # Entry point
    ├── public/           # Static files
    ├── package.json      # Node.js dependencies
    ├── tsconfig.json     # TypeScript configuration
    └── Dockerfile        # Frontend Docker configuration
```

## Adding DAGs

Place your DAG files (`.py` files) in the `dags/` directory. They will be automatically loaded by Airflow.

Example: Create `dags/example_dag.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    
    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    
    t1 >> t2
```

## Adding Python Dependencies

1. Add your packages to `requirements.txt`
2. Rebuild the Docker image:
   ```bash
   docker-compose build
   docker-compose up -d
   ```

## Useful Commands

- **View logs**: `docker-compose logs -f`
- **View scheduler logs**: `docker-compose logs -f airflow-scheduler`
- **View webserver logs**: `docker-compose logs -f airflow-webserver`
- **View frontend logs**: `docker-compose logs -f frontend`
- **Restart services**: `docker-compose restart`
- **Restart frontend only**: `docker-compose restart frontend`
- **Remove everything** (including volumes): `docker-compose down -v`

## Services

- **airflow-webserver**: Web UI (port 8080)
- **airflow-scheduler**: Scheduler service
- **postgres**: PostgreSQL database (port 5432)
  - `airflow` database: Airflow metadata
  - `dag_data` database: Your DAG data storage
- **pgadmin**: Database management UI (port 5050)
- **frontend**: React + TypeScript frontend dashboard (port 3000)
  - Connects to PostgreSQL database to display data from Airflow pipelines
  - Hot reload enabled for development

## Database Setup

### Database Tables and Views

The project uses two main tables for storing neuroscience datasets:

1. **`dandi_dataset`**: Stores datasets fetched from DANDI Archive API
   - Created by: `dandi_ingestion` DAG
   - Columns: `dataset_id`, `title`, `modality`, `citations`, `url`, `description`, `created_at`, `updated_at`, `version`

2. **`neuroscience_datasets`**: Stores datasets from other sources (Kaggle, OpenNeuro, PhysioNet)
   - Created by: `populate_neuroscience_datasets` DAG
   - Columns: `source`, `dataset_id`, `title`, `modality`, `citations`, `url`, `description`, `created_at`, `updated_at`

3. **`unified_datasets`** (VIEW): A SQL view that combines data from both tables
   - Automatically created by both DAGs
   - Provides a unified interface to query all datasets regardless of source
   - The API uses this view by default (falls back to `neuroscience_datasets` table if view doesn't exist)

### Using the Database in Your DAGs

The project includes utility functions for database operations and environment detection:

```python
from utils.environment import is_local_environment, get_database_config
from utils.database import execute_query, execute_update

# Detect if running locally or hosted
if is_local_environment():
    print("Running locally - using local PostgreSQL")
    # Your local logic here
else:
    print("Running in hosted environment")
    # Your hosted logic here

# Execute queries
results = execute_query("SELECT * FROM my_table LIMIT 10")
execute_update("INSERT INTO my_table (col1, col2) VALUES (%(val1)s, %(val2)s)", 
               {'val1': 'data1', 'val2': 'data2'})
```

### Environment Detection

The `utils/environment.py` module automatically detects if you're running locally or in a hosted environment (GCP, AWS, Azure, etc.) and configures the database connection accordingly.

### Example DAGs

- `example_dag.py`: Basic Airflow example
- `database_example_dag.py`: Demonstrates database operations with environment detection
- `dandi_ingestion.py`: Fetches and ingests datasets from DANDI Archive
- `populate_datasets_dag.py`: Populates neuroscience datasets from multiple sources

## API Backend

The project includes a FastAPI backend that provides REST endpoints for accessing neuroscience datasets.

### Accessing the API

- **API Documentation (Swagger UI)**: http://localhost:8000/docs
- **Alternative Docs (ReDoc)**: http://localhost:8000/redoc
- **Base URL**: http://localhost:8000

### Available Endpoints

- `GET /` - Health check
- `GET /api/health` - Database health check (includes view status)
- `GET /api/datasets` - Fetch datasets with optional filters (source, modality, search)
- `GET /api/datasets/stats` - Get dataset statistics
- `POST /api/refresh-view` - Manually create or refresh the unified_datasets view
- `GET /api/debug/view-info` - Debug endpoint to check view status and data sources

For detailed information on using the API, including how to test endpoints with the interactive documentation and curl commands, see [docs/API_USAGE.md](docs/API_USAGE.md).

### Database Schema and Unified View

The backend uses a **unified view** (`unified_datasets`) that combines data from multiple source tables:

- **`dandi_dataset`**: Datasets from DANDI Archive
- **`neuroscience_datasets`**: Datasets from Kaggle, OpenNeuro, and PhysioNet

The `unified_datasets` view automatically combines data from both tables using a SQL UNION, making it easy to query all datasets regardless of their source. The view is automatically created/updated when:
- The `populate_neuroscience_datasets` DAG runs (after table creation)
- The `dandi_ingestion` DAG runs (after DANDI data insertion)

**If you only see data from one source in the frontend:**
1. Check view status: `GET http://localhost:8000/api/debug/view-info`
2. Refresh the view: `POST http://localhost:8000/api/refresh-view`
3. Verify both tables have data in pgAdmin
4. Check API logs to see which table/view is being queried

## Frontend Development

The React frontend is set up with TypeScript and connects to the FastAPI backend to display neuroscience datasets.

### Frontend Structure

- **Main component**: `frontend/src/App.tsx` - Main React component
- **Hot reload**: Enabled in Docker for development

### Development

- The frontend automatically starts with `docker-compose up -d`
- Changes to frontend code are hot-reloaded (no need to restart)
- Access the frontend at: http://localhost:3000

## Troubleshooting

- **Permission errors on Linux/Mac**: Make sure `AIRFLOW_UID` in `.env` matches your user ID
- **Port 8080 already in use**: Change the port in `docker-compose.yml` under `airflow-webserver` ports
- **Port 3000 already in use**: Change the port in `docker-compose.yml` under `frontend` ports
- **DAGs not appearing**: Check the scheduler logs and ensure your DAG files are in the `dags/` directory
- **Frontend not loading**: Check frontend logs with `docker-compose logs -f frontend`
- **Only seeing data from one source in frontend**:
  - Check if the `unified_datasets` view exists: Visit `http://localhost:8000/api/debug/view-info`
  - Refresh the view: `POST http://localhost:8000/api/refresh-view` (use curl or Postman)
  - Verify both `dandi_dataset` and `neuroscience_datasets` tables have data in pgAdmin
  - Check backend logs to see which table/view is being queried
  - Ensure both DAGs have run successfully (`dandi_ingestion` and `populate_neuroscience_datasets`)
- **API connection errors**: 
  - Check if backend is running: `docker-compose logs -f api`
  - Verify database connection in `/api/health` endpoint
  - Check if tables exist in pgAdmin
- **Empty datasets in frontend**:
  - The frontend now shows a "No Datasets Found" message with a retry button if the database is empty
  - Run the DAGs to populate data: `populate_neuroscience_datasets` and `dandi_ingestion`

## Notes

- This setup uses the **LocalExecutor**, which is suitable for local development
- For production, consider using **CeleryExecutor** or **KubernetesExecutor**
- The database is stored in a Docker volume and persists between restarts
- To reset everything, run `docker-compose down -v` (this deletes the database)

