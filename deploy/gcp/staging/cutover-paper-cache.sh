#!/usr/bin/env bash
# One-time cutover of the staging Airflow VM from the old airflow-output Docker
# volume to the paper-mapping bucket (gcsfuse):
#
#   1. sync the checkout to origin/main (re-runs itself if the script changed)
#   2. stop Airflow (running DAG runs are killed; this is staging)
#   3. copy the volume to the bucket (incremental: only what changed since the
#      last copy)
#   4. mount the bucket, spot-check files on the mount, and only then write
#      the cutover marker
#   5. start Airflow and wait for the API server to be healthy
#
# The marker means "the old volume was copied and verified". Once it exists the
# script never copies again: the bucket is then newer than the old volume, and a
# second copy would overwrite fresh papers with stale ones. A run with the
# marker present only remounts the bucket and starts Airflow, so it doubles as
# the recovery command if the mount or the stack is ever down.
#
# From your machine:
#   gcloud compute ssh neuro-d3-airflow --zone us-west1-a --tunnel-through-iap \
#     --command "sudo bash /opt/neuro-d3/deploy/gcp/staging/cutover-paper-cache.sh"
#
# Safe to re-run, before or after the marker exists. It keeps going if the SSH session drops
# (output is also in /var/log/neuro-d3-paper-cache-cutover.log). It never
# deletes the old volume; that stays a manual step once you are confident.
set -euo pipefail

if [[ $# -gt 0 ]]; then
  sed -n '2,18p' "$0"
  exit 0
fi

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo bash $0" >&2
  exit 1
fi

APP_DIR="/opt/neuro-d3"
ENV_FILE="/etc/neuro-d3/airflow.env"
WRAPPER="$APP_DIR/deploy/gcp/staging/airflow-compose.sh"
MOUNT_SCRIPT="$APP_DIR/deploy/gcp/staging/mount-output-bucket.sh"
MOUNT_POINT="/mnt/airflow-output"
LEGACY_VOLUME="neuro-d3_airflow-output"
CUTOVER_MARKER="/etc/neuro-d3/paper-cache-cutover-done"
LOG_FILE="/var/log/neuro-d3-paper-cache-cutover.log"

# Survive a dropped SSH session: ignore HUP (inherited by tee and every child),
# and let tee keep writing the log if the terminal goes away.
trap '' HUP
if [[ -z "${CUTOVER_REEXEC:-}" ]]; then
  exec > >(tee -a --output-error=warn "$LOG_FILE") 2>&1
fi

step() { echo; echo "=== [$(date -u +%H:%M:%S)] $* ==="; }
die()  { echo "ERROR: $*" >&2; exit 1; }

AIRFLOW_STOPPED=0
on_exit() {
  local rc=$?
  if [[ $rc -ne 0 && $AIRFLOW_STOPPED -eq 1 ]]; then
    echo >&2
    echo "Airflow is STOPPED. Fix the error above and re-run this script; it picks up" >&2
    echo "where it left off. Log: $LOG_FILE" >&2
  fi
}
trap on_exit EXIT

[[ -f "$ENV_FILE" ]] || die "$ENV_FILE not found (written by the VM startup script)."
BUCKET="$(grep -m1 '^DATA_BUCKET=' "$ENV_FILE" | cut -d= -f2- || true)"
[[ -n "$BUCKET" ]] || die "DATA_BUCKET is not set in $ENV_FILE."

# ─── 1. Checkout = origin/main ───────────────────────────────────────────────
step "1/5 Sync $APP_DIR to origin/main"
git -C "$APP_DIR" fetch --quiet origin main
if [[ "$(git -C "$APP_DIR" rev-parse HEAD)" != "$(git -C "$APP_DIR" rev-parse FETCH_HEAD)" ]]; then
  git -C "$APP_DIR" checkout -B main FETCH_HEAD
  if [[ -z "${CUTOVER_REEXEC:-}" ]]; then
    echo "Checkout updated; re-running the new copy of this script."
    CUTOVER_REEXEC=1 exec bash "$APP_DIR/deploy/gcp/staging/cutover-paper-cache.sh"
  fi
fi
git -C "$APP_DIR" log --oneline -1
[[ -f "$MOUNT_SCRIPT" ]] || die "$MOUNT_SCRIPT is missing on main. Is the GCS cache PR merged?"

start_on_bucket() {
  bash "$MOUNT_SCRIPT" "$BUCKET"
  mountpoint -q "$MOUNT_POINT" || die "$MOUNT_POINT is not mounted."
  bash "$WRAPPER" up -d
  AIRFLOW_STOPPED=0

  echo "Waiting for the API server to report healthy (up to 5 min)..."
  local status=starting cid
  for _ in $(seq 1 30); do
    cid="$(bash "$WRAPPER" ps -q airflow-api-server 2>/dev/null || true)"
    if [[ -n "$cid" ]]; then
      status="$(docker inspect -f '{{ .State.Health.Status }}' "$cid" 2>/dev/null || echo starting)"
    fi
    [[ "$status" == "healthy" ]] && break
    sleep 10
  done
  bash "$WRAPPER" ps
  [[ "$status" == "healthy" ]] || die "API server is not healthy yet (status: $status). Check: sudo bash $WRAPPER logs airflow-api-server"
}

# ─── Nothing to migrate: fresh VM, or old volume already removed ────────────
if ! docker volume inspect "$LEGACY_VOLUME" >/dev/null 2>&1; then
  step "No $LEGACY_VOLUME volume: nothing to migrate, mount and start"
  start_on_bucket
  exit 0
fi

# ─── Already migrated: never copy the old volume again ──────────────────────
if [[ -f "$CUTOVER_MARKER" ]]; then
  step "Cutover already done ($CUTOVER_MARKER exists): mount and start only, no copy"
  start_on_bucket
  echo
  echo "Once you are confident: sudo docker volume rm $LEGACY_VOLUME"
  exit 0
fi

SRC="$(docker volume inspect -f '{{ .Mountpoint }}' "$LEGACY_VOLUME")"
[[ -d "$SRC" ]] || die "Volume path $SRC does not exist."

# ─── 2. Stop ─────────────────────────────────────────────────────────────────
step "2/5 Stop Airflow"
bash "$WRAPPER" down
AIRFLOW_STOPPED=1

# ─── 3. Copy ─────────────────────────────────────────────────────────────────
# fetcher writes .tmp-*.json then renames; a leftover one is never read.
step "3/5 Copy the volume to gs://$BUCKET (incremental)"
gcloud storage rsync -r --exclude='(^|.*/)\.tmp-[^/]*$' "$SRC" "gs://$BUCKET/"

# ─── 4. Mount, spot check, then marker ──────────────────────────────────────
step "4/5 Mount gs://$BUCKET at $MOUNT_POINT and verify the copy"
bash "$MOUNT_SCRIPT" "$BUCKET"
mountpoint -q "$MOUNT_POINT" || die "$MOUNT_POINT is not mounted."

echo "Spot-checking 25 random files from the volume on the mount..."
missing=0
while IFS= read -r f; do
  rel="${f#"$SRC"/}"
  if [[ ! -f "$MOUNT_POINT/$rel" ]] || [[ "$(stat -c %s "$f")" != "$(stat -c %s "$MOUNT_POINT/$rel")" ]]; then
    echo "  MISSING or different: $rel"
    missing=$((missing + 1))
  fi
done < <(find "$SRC" -type f ! -name '.tmp-*' | shuf -n 25)
if [[ $missing -gt 0 ]]; then
  die "$missing of 25 sampled files are missing or differ on the mount. Not starting Airflow."
fi
echo "All 25 present with matching sizes."
touch "$CUTOVER_MARKER"
echo "Wrote $CUTOVER_MARKER: from now on this script will not copy the old volume again."

# ─── 5. Start ────────────────────────────────────────────────────────────────
step "5/5 Start Airflow on the bucket"
start_on_bucket

step "Done"
cat <<EOF
Paper cache and run artifacts now live in gs://$BUCKET, mounted at $MOUNT_POINT.

Verify: trigger one paper-mapping DAG on an already-mapped dataset and check its
log shows cache hits, not refetches.

Once you are confident, reclaim the boot disk (about 10 GB):
  sudo docker volume rm $LEGACY_VOLUME
EOF
