#!/usr/bin/env bash
# Safe wrapper for the staging Airflow compose stack on the GCE VM.
#
# Always targets the GCE compose file with the correct project directory (so
# ./airflow/dags, ./airflow/config, ./deploy/... resolve) and the secrets env file the startup
# script wrote. Use this for EVERY manual op on the VM, e.g.:
#
#   sudo bash /opt/neuro-d3/deploy/gcp/staging/airflow-compose.sh ps
#   sudo bash /opt/neuro-d3/deploy/gcp/staging/airflow-compose.sh up -d --force-recreate airflow-api-server
#
# Never run a bare `docker compose` from /opt/neuro-d3: with no -f it selects the
# repo-root local-dev docker-compose.yml, which ships a `postgres` service and a
# hardcoded airflow:airflow@postgres conn — the phantom DB behind the split-brain.
set -euo pipefail

APP_DIR="/opt/neuro-d3"
ENV_FILE="/etc/neuro-d3/airflow.env"
COMPOSE_FILE="$APP_DIR/deploy/gcp/staging/docker-compose.gce.yml"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. It is written by the VM startup script from" >&2
  echo "       Secret Manager; without it the stack cannot be configured." >&2
  exit 1
fi

exec docker compose \
  --project-directory "$APP_DIR" \
  --env-file "$ENV_FILE" \
  -f "$COMPOSE_FILE" \
  "$@"
