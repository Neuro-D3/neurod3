# NeuroD3 — staging deployment plan (GCP)

Official, ordered runbook for standing up the **staging** environment in project
`neuro-d3-staging` (`us-west1`). Read [README.md](README.md) for the architecture;
this file is the step-by-step apply procedure.

> **Boundary:** this provisions only resources *inside* the project. The project,
> folder, billing, API enablement, and the Terraform state bucket are owned by
> `Dura-Labs/duralabs-infra` and are never touched here.

`terraform plan` currently shows **51 to add, 0 change, 0 destroy.**

---

## 0. Prerequisites (one-time)

- **Tools:** Terraform `~> 1.15`, `gcloud`, Docker (for building images).
- **Auth (ADC):**
  ```powershell
  gcloud auth application-default login
  ```
  - If gcloud errors *"python not found"*:
    `$env:CLOUDSDK_PYTHON = "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\platform\bundledpython\python.exe"`
  - Tokens expire (`invalid_rapt`) — re-run the login when a command complains.
- **Variables:** `terraform.tfvars` exists (gitignored). Confirm the `*_image` refs
  and tier/sizing. Real secret values can wait until step 3.
- **Init (if not already):**
  ```powershell
  terraform init
  ```

**Why this is staged:** Cloud Run validates the container image *at deploy time* and
fails if it isn't in the registry yet. So: create the registry → push images →
apply the rest. Expect **3–4 applies total** (bootstrap, full, CORS, and any secret
rotation) — this is normal, not a mistake.

---

## 1. Phase 0 — bootstrap the Artifact Registry

```powershell
terraform apply -target=google_artifact_registry_repository.neuro_d3
```
Creates only the `neuro-d3` Docker repo so images have somewhere to go.

---

## 2. Phase 1 — build & push the three images

```powershell
$REPO = "us-west1-docker.pkg.dev/neuro-d3-staging/neuro-d3"
gcloud auth configure-docker us-west1-docker.pkg.dev

docker build -t "$REPO/api:bootstrap"      ./api      ; docker push "$REPO/api:bootstrap"
docker build -t "$REPO/airflow:bootstrap"  ./airflow  ; docker push "$REPO/airflow:bootstrap"
docker build -t "$REPO/frontend:bootstrap" ./frontend ; docker push "$REPO/frontend:bootstrap"
```
- Build order doesn't matter — the frontend reads `REACT_APP_API_URL` at *container
  start* (CRA dev server), not at build time.
- The `airflow` image build now pip-installs `apache-airflow-providers-google`
  (for GCS task-log remote logging) — it's a larger build; that's expected.
- Tags must match `terraform.tfvars`. Prefer immutable `@sha256:` digests once you
  iterate (re-pushing the *same tag* won't trigger a Cloud Run redeploy).

---

## 3. Phase 2 — full apply

```powershell
terraform apply
```
Creates everything else. Notes:
- **Cloud SQL takes ~10–15 min** to create the first time.
- The **VM apply succeeds even if the airflow image lookup is momentarily behind** —
  the startup script's `docker pull` fails gracefully and logs; it comes up once the
  image is present.
- IAM is wired with `depends_on` so secret/SQL access exists before the consumers
  start (avoids first-deploy permission races).

Capture the outputs:
```powershell
terraform output
```

---

## 4. Phase 3 — post-apply configuration

**a. Real secrets** (placeholders won't work):
```powershell
# Airflow Fernet key — REQUIRED before Airflow will boot.
$fernet = python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
$fernet | gcloud secrets versions add d3-staging-airflow-fernet-key --data-file=- --project neuro-d3-staging

# OpenRouter API key (for the paper_reuse_classification DAG).
"sk-or-v1-..." | gcloud secrets versions add d3-staging-openrouter-api-key --data-file=- --project neuro-d3-staging
```
The VM reads `latest` at boot, so restart Airflow after adding these. On the VM,
**always restart through the wrapper** — never a bare `docker compose` (with no
`-f` it selects the repo-root local-dev compose and its phantom Postgres, which
silently split-brains the metadata DB and shows zero DAGs):

```powershell
gcloud compute ssh neuro-d3-airflow --zone us-west1-a --tunnel-through-iap
# then, on the VM:
sudo bash /opt/neuro-d3/deploy/gcp/staging/airflow-compose.sh up -d
```

**b. API CORS** (cross-origin: frontend and API are different `*.run.app` hosts):
```powershell
# Set allowed_origins to the frontend_url output, then re-apply.
terraform output -raw frontend_url   # copy this
# edit terraform.tfvars: allowed_origins = "<frontend_url>"
terraform apply
```

**c. Database schema** (app-level, not Terraform): run Alembic against the
`dag_data` DB to create the `neuroscience_datasets` tables (Cloud SQL only creates
the empty databases). Connect via the Cloud SQL Auth Proxy or `gcloud sql connect`.

---

## 5. Phase 4 — verify

```powershell
$api = terraform output -raw api_url
curl "$api/api/health"          # -> DB-connected status
curl "$api/api/datasets"        # -> rows once the schema is seeded

terraform output -raw frontend_url   # open in a browser; check it calls the API (network tab)
terraform output -raw airflow_url    # https://<static-ip> — self-signed cert warning is expected
```
- **Airflow UI:** `https://<static-ip>` (Caddy, self-signed → browser warning). Log
  in with `airflow` / the `d3-staging-airflow-admin-password` secret.
- **VM debugging:** `gcloud compute ssh neuro-d3-airflow --zone us-west1-a --tunnel-through-iap`
  then `sudo cat /var/log/neuro-d3-startup.log` and
  `sudo bash /opt/neuro-d3/deploy/gcp/staging/airflow-compose.sh ps`.
- **Logs in GCS:** `gsutil ls gs://neuro-d3-staging-airflow-logs/airflow-logs/` after a task runs.

---

## 6. Iterating & teardown

- **New image:** merge to `main` (or run **Deploy to Staging** by hand with
  `deploy_all`) and the workflow builds, pushes and rolls out the changed
  components. Terraform ignores the Cloud Run image after the first apply, so
  changing `api_image` / `frontend_image` in `terraform.tfvars` does **not**
  redeploy. To roll an image by hand, push a new tag/digest, then:
  ```powershell
  gcloud run services update neuro-d3-api      --image <ref> --region us-west1 --project neuro-d3-staging
  gcloud run services update neuro-d3-frontend --image <ref> --region us-west1 --project neuro-d3-staging
  ```
- **Teardown:** `terraform destroy` removes only in-project resources, never the
  platform-owned project/state bucket. `db_deletion_protection = false` and the
  logs bucket's `force_destroy = true` let it run cleanly; the paper-mapping
  bucket has `force_destroy = false` on purpose, so destroying it means emptying
  it by hand first. Its contents (cached papers) are the one piece of staging
  state that is slow and costly to rebuild.

---

## Persistence (what survives a VM rebuild)

| State | Location | Durable? |
|---|---|---|
| Run history / task state | Cloud SQL (`airflow` DB) | ✅ |
| App data (datasets, mappings) | Cloud SQL (`dag_data` DB) | ✅ |
| Task logs | GCS (`…-airflow-logs`) | ✅ |
| DAG code | git (re-cloned on boot) | ✅ |
| Secrets | Secret Manager | ✅ |
| Paper cache + run artifacts | GCS (`…-paper-mapping`), gcsfuse-mounted on the VM | ✅ |


---

## Paper cache cutover (one-time, moving an existing VM onto the bucket)

The containers' `/opt/airflow/output` (paper-text-fetcher cache, the four
`*_paper_mapping` output dirs) is a bind mount of `/mnt/airflow-output`, which
`mount-output-bucket.sh` makes the `…-paper-mapping` bucket with gcsfuse. A fresh
VM gets this from the startup script. A VM that already has papers in the old
`airflow-output` Docker volume is moved over like this. `airflow-compose.sh`
(which CI deploys and the startup script both go through) refuses to start the
stack until the mount exists **and**, while the old `neuro-d3_airflow-output`
Docker volume still exists, until `/etc/neuro-d3/paper-cache-cutover-done` says
the final copy was made. So merging this change, or rebooting the VM, before the
cutover only makes the deploy or the boot fail loudly; neither can send the
cache to the boot disk or start on a bucket that is missing recent papers.

Staging's cutover is done once, with a one-off script kept out of the repo
that runs steps 2–6 below in order. These steps are the record of what it
does, and what to follow by hand on any other VM that still has the old volume:

1. **Terraform apply** (lifecycle rule off, startup script). Safe while DAGs run:
   the startup-script change is a metadata update and does not reboot the VM.
2. **First copy, Airflow still running.** Moves the bulk so the outage later is
   short. `gcloud storage rsync` is incremental, so re-running it only uploads
   what changed.
   ```bash
   gcloud compute ssh neuro-d3-airflow --zone us-west1-a --tunnel-through-iap
   ```
   ```bash
   sudo gcloud storage rsync -r /var/lib/docker/volumes/neuro-d3_airflow-output/_data gs://neuro-d3-staging-paper-mapping/
   ```
3. **Optional: let running paper-mapping DAGs finish.** Staging keeps no state
   worth protecting, so killing them is fine. paper-text-fetcher writes its cache
   atomically, and a truncated mapping-DAG text file reads as a cache miss.
4. **Stop Airflow, final copy with nothing writing, mark the cutover done,
   pull main:**
   ```bash
   sudo bash /opt/neuro-d3/deploy/gcp/staging/airflow-compose.sh down      && sudo gcloud storage rsync -r /var/lib/docker/volumes/neuro-d3_airflow-output/_data gs://neuro-d3-staging-paper-mapping/      && sudo touch /etc/neuro-d3/paper-cache-cutover-done
   sudo git -C /opt/neuro-d3 fetch origin main && sudo git -C /opt/neuro-d3 checkout -B main origin/main
   ```
   The three are chained so the marker is written only if the stop and the copy
   both succeeded. The marker is what lets `airflow-compose.sh up` proceed while
   the old volume is still on disk. Only create it after a final rsync made with Airflow down,
   and **never rsync the old volume again once it exists**: from then on the
   bucket is newer, and a copy would overwrite fresh papers with stale ones.
5. **Mount the bucket and start Airflow:**
   ```bash
   sudo bash /opt/neuro-d3/deploy/gcp/staging/mount-output-bucket.sh
   ls /mnt/airflow-output        # paper_text_fetcher/ and the four *_paper_mapping/ dirs
   sudo bash /opt/neuro-d3/deploy/gcp/staging/airflow-compose.sh up -d
   ```
6. **Verify:** trigger one paper-mapping DAG on a dataset whose papers were
   already cached. Its task log should show cache hits and no refetches, and
   `gcloud storage ls gs://neuro-d3-staging-paper-mapping/paper_text_fetcher/ | head`
   should list objects. New objects appear in the bucket as later runs write.
7. **Later, once confident:** `sudo docker volume rm neuro-d3_airflow-output`
   reclaims the boot-disk space. Nothing reads it any more, and with the volume
   gone the marker file is no longer consulted.

Performance notes: a write is one object upload on `close()` (about 50–150 ms,
streamed while the file is written), small against the roughly 7 s each citing
paper takes to fetch. Reads are one GET, or a boot-disk hit once gcsfuse's file
cache (4 GB cap, `/var/cache/gcsfuse`) has the object. gcsfuse caches "not
found" for 5 s, so two parallel tasks can still fetch the same paper within a few
seconds of each other, the same harmless double download the local volume had.

### If the gcsfuse mount dies

If gcsfuse exits, `/mnt/airflow-output` becomes a dead mount: reads fail with
"Transport endpoint is not connected" and paper-mapping tasks fail. Either
reboot the VM (fstab mounts the bucket before Docker starts, then the startup
script brings Airflow up), or without a reboot:

```bash
sudo bash /opt/neuro-d3/deploy/gcp/staging/airflow-compose.sh down
sudo bash /opt/neuro-d3/deploy/gcp/staging/mount-output-bucket.sh
sudo bash /opt/neuro-d3/deploy/gcp/staging/airflow-compose.sh up -d
```

Stop Airflow first: running containers keep the dead mount until they are
recreated. `mount-output-bucket.sh` detects a mount that no longer answers,
detaches it and mounts again. Nothing is lost: everything written before the
crash is already in the bucket.

---

## Known follow-ups (not in this deploy)

- **Airflow TLS:** self-signed today; point a hostname at the static IP and flip the
  Caddyfile to Let's Encrypt (no infra change).
- **CI/Workload Identity Federation:** intentionally deferred.
- **Static frontend build:** optional perf/cost optimization over the dev server.
