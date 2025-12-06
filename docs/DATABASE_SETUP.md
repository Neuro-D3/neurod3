# Database Connection Setup

## Architecture

```
Frontend (React) :3000
    ↓ HTTP
Backend API (FastAPI) :8000
    ↓ PostgreSQL
Database (dag_data) :5432
```

## Quick Start

1. **Start services**:
   ```bash
   docker-compose up -d
   ```

2. **Populate database**:
   - Open http://localhost:8080 (Airflow)
   - Login: `airflow` / `airflow`
   - Enable and trigger `populate_neuroscience_datasets` DAG

3. **Access frontend**:
   - Open http://localhost:3000

## API Endpoints

- `GET /api/datasets?source=&modality=&search=` - Fetch datasets
- `GET /api/datasets/stats` - Get statistics
- `GET /api/health` - Health check

## Database Connection

**Config** (in `docker-compose.yml`):
```yaml
DB_HOST=postgres
DB_PORT=5432
DB_NAME=dag_data
DB_USER=airflow
DB_PASSWORD=airflow
```

**Table**: `neuroscience_datasets`
- Columns: source, dataset_id, title, modality, citations, url, description
- Unique constraint on (source, dataset_id)

## Database Management with pgAdmin

pgAdmin is available at http://localhost:5050 for visual database management.

**Default credentials**:
- Email: `admin@admin.com`
- Password: `admin`

### Automatic Server Configuration

The pgAdmin container is pre-configured with server connections via `database/pgadmin-servers.json`. Servers are automatically added on first startup.

**Important Notes**:
- Password fields are not imported for security reasons
- On first connection, enter the password (`airflow`), which will then be saved
- The configuration file is mounted to `/pgadmin4/servers.json`

### Manual Server Addition

If automatic configuration doesn't work, manually add servers in pgAdmin:

1. Right-click "Servers" → "Register" → "Server"
2. Use these settings:
   - **Host**: `postgres` (not localhost!)
   - **Port**: `5432`
   - **Database**: `airflow` or `dag_data`
   - **Username**: `airflow`
   - **Password**: `airflow`

### pgAdmin Troubleshooting

If servers don't appear:

1. **Clear browser cache** - pgAdmin caches server lists
2. **Hard refresh** - Press Ctrl+Shift+R (or Cmd+Shift+R on Mac)
3. **Check logs** - `docker-compose logs pgadmin | grep -i "server\|config"`
4. **Manual verification** - Check if file exists:
   ```bash
   docker-compose exec pgadmin cat /var/lib/pgadmin/storage/admin@admin.com/servers.json
   ```
5. **Force reinstall** - Set `FORCE_OVERWRITE=true` in docker-compose.yml and restart

## Troubleshooting

### Frontend shows connection error
```bash
# Check API health
curl http://localhost:8000/api/health

# Check logs
docker-compose logs api

# Restart services
docker-compose restart api frontend
```

### Database is empty
```bash
# Trigger DAG in Airflow UI, or:
docker-compose exec postgres psql -U airflow -d dag_data -c "SELECT COUNT(*) FROM neuroscience_datasets;"
```

### API not responding
```bash
docker-compose logs api --tail=50
docker-compose restart api
```
