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

# /opt/airflow/output in the containers is a bind mount of /mnt/airflow-output,
# which mount-output-bucket.sh makes the paper-mapping bucket. Starting the stack
# while it is a plain directory would send the paper cache to the boot disk with
# no error, so refuse. (CI deploys run `up -d` through this wrapper too.)
case "${1:-}" in
  up|start|restart|run)
    if ! mountpoint -q /mnt/airflow-output; then
      echo "ERROR: /mnt/airflow-output is not a mountpoint. Mount the paper-mapping" >&2
      echo "       bucket first: sudo bash $APP_DIR/deploy/gcp/staging/mount-output-bucket.sh" >&2
      exit 1
    fi
    ;;
esac

exec docker compose \
  --project-directory "$APP_DIR" \
  --env-file "$ENV_FILE" \
  -f "$COMPOSE_FILE" \
  "$@"
