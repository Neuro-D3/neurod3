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
# no error, so refuse. (CI deploys and the VM startup script both run `up -d`
# through this wrapper.)
#
# The check runs for every subcommand except the ones listed below that never
# start a container, so a global option in front (`--ansi never up`) or an
# unknown subcommand still gets checked.
LEGACY_VOLUME="neuro-d3_airflow-output"            # project name = basename of APP_DIR
CUTOVER_MARKER="/etc/neuro-d3/paper-cache-cutover-done"

require_output_mount() {
  if ! mountpoint -q /mnt/airflow-output; then
    echo "ERROR: /mnt/airflow-output is not a mountpoint. Mount the paper-mapping" >&2
    echo "       bucket first: sudo bash $APP_DIR/deploy/gcp/staging/mount-output-bucket.sh" >&2
    exit 1
  fi
  # A VM that still has the pre-GCS Docker volume is mid-migration: the bucket
  # may be missing papers written since the last rsync, and fulltext_cache_key
  # rows point at them. Refuse until the operator has done the final offline
  # rsync and said so (DEPLOY.md, "Paper cache cutover"). A fresh VM has no
  # such volume and starts normally.
  if [[ ! -f "$CUTOVER_MARKER" ]] && docker volume inspect "$LEGACY_VOLUME" >/dev/null 2>&1; then
    echo "ERROR: the old $LEGACY_VOLUME Docker volume still exists and the paper-cache" >&2
    echo "       cutover is not marked done. With Airflow stopped, run the final" >&2
    echo "       gcloud storage rsync (DEPLOY.md), then: sudo touch $CUTOVER_MARKER" >&2
    exit 1
  fi
}

case "${1:-}" in
  down|stop|kill|rm|ps|logs|config|pull|images|version|events|top|port|exec|ls|help|--help|-h|--version) ;;
  *) require_output_mount ;;
esac

exec docker compose \
  --project-directory "$APP_DIR" \
  --env-file "$ENV_FILE" \
  -f "$COMPOSE_FILE" \
  "$@"
