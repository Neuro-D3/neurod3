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
