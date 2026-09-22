# API Usage Guide

This guide explains how to use the NeuroD3 FastAPI backend to access neuroscience datasets.

## Quick Start

1. **Start the services**:
   ```bash
   docker-compose up -d
   ```

2. **Access the API documentation**:
   - Open http://localhost:8000/docs in your browser

3. **Test an endpoint**:
   ```bash
   curl http://localhost:8000/api/health
   ```

## Interactive API Documentation

FastAPI automatically generates interactive API documentation that you can use to test endpoints directly in your browser.

### Swagger UI (Recommended)

Access: http://localhost:8000/docs

#### How to Use the Interactive Docs

1. **Browse Available Endpoints**:
   - All endpoints are listed with their HTTP methods (GET, POST, etc.)
   - Click on any endpoint to expand it and see details

2. **Try Out an Endpoint**:
   - Click on an endpoint to expand it
   - Click the **"Try it out"** button in the top right
   - Fill in any required or optional parameters
   - Click **"Execute"** to send the request
   - View the response below, including:
     - Response body (JSON)
     - Response status code
     - Response headers
     - The curl command equivalent

3. **Example: Fetching Datasets**:
   - Expand `GET /api/datasets`
   - Click "Try it out"
   - Enter optional filters:
     - **source**: `DANDI` (or `Kaggle`, `OpenNeuro`, `PhysioNet`)
     - **modality**: `fMRI` (or `EEG`, `MRI`, etc.)
     - **search**: `brain` (searches in title and description)
   - Click "Execute"
   - View the returned datasets in JSON format

4. **Copy curl Command**:
   - After executing a request, scroll down to see the exact curl command used
   - Copy this command to use in your terminal or scripts

### ReDoc (Alternative)

Access: http://localhost:8000/redoc

ReDoc provides a cleaner, read-only documentation interface:
- Three-panel layout with navigation
- Better for browsing and understanding the API
- No interactive testing (use Swagger UI for that)

## Using curl

curl is a command-line tool for making HTTP requests. Here are examples for common use cases.

### Basic Health Check

```bash
curl http://localhost:8000/
```

Response:
```json
{
  "status": "ok",
  "message": "NeuroD3 API is running"
}
```

### Database Health Check

```bash
curl http://localhost:8000/api/health
```

### Fetch All Datasets

```bash
curl http://localhost:8000/api/datasets
```

### Filter by Source

```bash
curl "http://localhost:8000/api/datasets?source=DANDI"
```

Available sources: `DANDI`, `Kaggle`, `OpenNeuro`, `PhysioNet`

### Filter by Modality

```bash
curl "http://localhost:8000/api/datasets?modality=fMRI"
```

Available modalities: `Behavioral`, `Calcium Imaging`, `Clinical`, `ECG`, `EEG`, `Electrophysiology`, `fMRI`, `MRI`, `Survey`, `X-ray`

### Search by Keyword

```bash
curl "http://localhost:8000/api/datasets?search=brain"
```

### Combine Multiple Filters

```bash
curl "http://localhost:8000/api/datasets?source=DANDI&modality=fMRI&search=cortex"
```

### Paper reuse classification fields

Each dataset in `GET /api/datasets` carries `reuse_count`: the number of distinct citing papers the LLM classified as having reused the dataset's data (`REUSE`, plus the retired `SECONDARY` label until the reclassification finishes). `secondary_reuse_count` is a deprecated alias with the same value and will be removed.

```bash
curl "http://localhost:8000/api/datasets?source=DANDI&sort_by=papers&limit=5" | jq '.[] | {id, reuse_count}'
```

`GET /api/datasets/{source}/{id}`, `GET /api/paper-mapping/datasets/{source}/{id}` and `GET /api/paper-mapping/citations` return one object per (citing paper, dataset) pair with:

| Field | Meaning |
|---|---|
| `classification` | `REUSE`, `MENTION`, `NEITHER` (citing mode) or `PRIMARY`, `REUSE`, `NEITHER` (direct mode); `null` until classified |
| `classification_status` | `classification`, else the row `status` (`placeholder`, `error`, `no_full_text`, `dry_run`), else `unclassified` |
| `mode`, `prompt_version`, `classification_model` | how the row was produced; rows with an older prompt version are reclassified on the next run |
| `confidence` | 1–10 |
| `reasoning` | the model's short justification |
| `evidence_quotes` | verbatim passages the model judged from, each `{quote, match_type, verbatim, chars, offset}`; `match_type` is `exact`, `normalized`, `case_insensitive`, `spacing_insensitive`, `punctuation_insensitive` or `not_found` |
| `hallucinated_quote_count` | quotes that were not found in the paper text |
| `reuse_type`, `reuse_type_other` | only for `REUSE`: `TOOL_DEMO`, `BENCHMARK`, `AGGREGATION`, `CONFIRMATORY`, `NOVEL_ANALYSIS`, `ML_TRAINING`, `SIMULATION`, `TEACHING`, `OTHER` (+ write-in) |
| `reused_modalities`, `reused_dandi_hosted` | only for `REUSE`: which parts of the data were reused (`neurophysiology`, `behavior`, `imaging`, `morphology`, `transcriptomics`, `other`, `unclear`) |
| `same_lab`, `same_lab_confidence`, `source_archive` | only for `REUSE` |
| `citing_text_status` | whether the citing paper's body was retrieved: `full_text`, `metadata_only`, `unavailable`, or `null` |

```bash
curl "http://localhost:8000/api/datasets/DANDI/000016" | jq '.citations[] | select(.classification == "REUSE") | {citing_paper_doi, confidence, reuse_type, reused_modalities, quote: .evidence_quotes[0].quote}'
```


### Get Dataset Statistics

```bash
curl http://localhost:8000/api/datasets/stats
```

Response:
```json
{
  "total": 100,
  "by_source": {
    "DANDI": 45,
    "OpenNeuro": 30
  },
  "by_modality": {
    "fMRI": 40,
    "EEG": 25
  }
}
```

## Troubleshooting

### Check API Logs

```bash
docker-compose logs api --tail=50 -f
```

### Verify Database Connection

```bash
curl http://localhost:8000/api/health
```

### Test Database Directly

```bash
docker-compose exec postgres psql -U airflow -d dag_data -c "SELECT COUNT(*) FROM neuroscience_datasets;"
```

### Restart Services

```bash
docker-compose restart api
```

## Error Responses

All errors are returned in JSON format:

```json
{
  "detail": "Error message here"
}
```

Common error codes:
- **400 Bad Request**: Invalid query parameter value
- **500 Internal Server Error**: Database connection or query error
- **503 Service Unavailable**: Database is not connected
