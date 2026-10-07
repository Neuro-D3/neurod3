#!/usr/bin/env bash
# Mount the paper-mapping bucket on the Airflow VM with gcsfuse.
#
# /opt/airflow/output inside the Airflow containers (paper-text-fetcher cache +
# the four *_paper_mapping output dirs) is a bind mount of /mnt/airflow-output on
# the host, and this script makes that path the bucket. Cached papers and run
# artifacts then live in GCS and survive a VM rebuild; the DAGs keep writing plain
# files to the same relative paths, so papers.fulltext_cache_key stays valid.
#
# Idempotent. Run by the VM startup script on every boot, and by hand once for the
# cutover (see DEPLOY.md, "Paper cache cutover"):
#
#   sudo bash /opt/neuro-d3/deploy/gcp/staging/mount-output-bucket.sh [bucket]
#
# The bucket defaults to DATA_BUCKET from /etc/neuro-d3/airflow.env.
set -euo pipefail

ENV_FILE="/etc/neuro-d3/airflow.env"
MOUNT_POINT="/mnt/airflow-output"
CACHE_DIR="/var/cache/gcsfuse"
AIRFLOW_UID="${AIRFLOW_UID:-50000}"

BUCKET="${1:-}"
if [[ -z "$BUCKET" && -f "$ENV_FILE" ]]; then
  BUCKET="$(grep -m1 '^DATA_BUCKET=' "$ENV_FILE" | cut -d= -f2- || true)"
fi
if [[ -z "$BUCKET" ]]; then
  echo "ERROR: no bucket given and DATA_BUCKET is not in $ENV_FILE" >&2
  exit 1
fi

# ─── 1. gcsfuse from Google's apt repo ───────────────────────────────────────
if ! command -v gcsfuse >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  CODENAME="$(. /etc/os-release && echo "$VERSION_CODENAME")"
  install -m 0755 -d /etc/apt/keyrings
  # gpg refuses to overwrite in batch mode; a keyring left by an earlier run
  # whose apt step failed must not block the retry.
  rm -f /etc/apt/keyrings/gcsfuse.gpg
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --batch --no-tty --dearmor -o /etc/apt/keyrings/gcsfuse.gpg
  echo "deb [signed-by=/etc/apt/keyrings/gcsfuse.gpg] https://packages.cloud.google.com/apt gcsfuse-${CODENAME} main" \
    > /etc/apt/sources.list.d/gcsfuse.list
  apt-get update
  apt-get install -y --no-install-recommends fuse3 gcsfuse
fi

# ─── Detach a dead mount first ──────────────────────────────────────────────
# If gcsfuse has exited, the kernel keeps a dead FUSE mount: every access fails
# with "Transport endpoint is not connected" (mkdir -p below included, which
# is why this runs first), and `mountpoint` may report it as
# mounted or fail outright. Look in the mount table instead, probe that the
# mount answers, and detach a dead one so it can be mounted again. Stop Airflow
# first (airflow-compose.sh down): running containers keep the dead mount until
# they are recreated.
if awk -v m="$MOUNT_POINT" '$2 == m { found = 1 } END { exit !found }' /proc/mounts \
   && ! timeout 20 ls "$MOUNT_POINT" >/dev/null 2>&1; then
  echo "WARNING: ${MOUNT_POINT} is mounted but not answering (gcsfuse exited?); detaching it"
  fusermount3 -uz "$MOUNT_POINT" 2>/dev/null || umount -l "$MOUNT_POINT"
fi

# ─── 2. fstab entry (mounted at boot, before docker.service) ─────────────────
# Options are gcsfuse flags with underscores (the mount helper turns them into
# --flags):
#   implicit_dirs   objects uploaded by rsync have no directory placeholders
#   allow_other     the containers' airflow user (50000) and root both use it
#   uid/gid         present every object as airflow:root, like the old volume
#   file cache      context extraction re-reads the same texts; serve them from
#                   the boot disk instead of a GET per read (4 GB cap)
# Metadata caching stays at the defaults (60 s positive, 5 s negative): two
# tasks can still fetch the same paper within a few seconds of each other, which
# is the same harmless double download the local volume had.
mkdir -p "$MOUNT_POINT" "$CACHE_DIR"
FSTAB_LINE="${BUCKET} ${MOUNT_POINT} gcsfuse rw,_netdev,allow_other,implicit_dirs,uid=${AIRFLOW_UID},gid=0,file_mode=644,dir_mode=755,cache_dir=${CACHE_DIR},file_cache_max_size_mb=4096 0 0"
if grep -qE "^[^#]*[[:space:]]${MOUNT_POINT}[[:space:]]" /etc/fstab; then
  sed -i -E "s|^[^#]*[[:space:]]${MOUNT_POINT}[[:space:]].*|${FSTAB_LINE}|" /etc/fstab
else
  echo "$FSTAB_LINE" >> /etc/fstab
fi

# docker.service must not start containers before the bucket is mounted, or the
# bind mount would be an empty boot-disk directory and the DAGs would write
# there without any error.
mkdir -p /etc/systemd/system/docker.service.d
cat > /etc/systemd/system/docker.service.d/wait-for-output-bucket.conf <<UNIT
[Unit]
RequiresMountsFor=${MOUNT_POINT}
UNIT
systemctl daemon-reload

# ─── 3. Mount now ────────────────────────────────────────────────────────────
if mountpoint -q "$MOUNT_POINT"; then
  echo "gs://${BUCKET} already mounted at ${MOUNT_POINT}"
else
  mount "$MOUNT_POINT"
  echo "Mounted gs://${BUCKET} at ${MOUNT_POINT}"
fi
ls -la "$MOUNT_POINT" | head -20
