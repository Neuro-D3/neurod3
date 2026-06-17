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

- **New image:** rebuild, push a **new tag/digest**, update `terraform.tfvars`,
  `terraform apply` (Cloud Run redeploys on the changed ref).
- **Teardown:** `terraform destroy` works cleanly (`db_deletion_protection = false`,
  buckets `force_destroy = true`). It removes only in-project resources, never the
  platform-owned project/state bucket. Exception: the `find-reuse-cache` bucket is
  `force_destroy = false` to protect the expensive-to-regenerate cache, so destroy
  fails while it holds objects. Empty it first (`gcloud storage rm --recursive
  gs://neuro-d3-staging-find-reuse-cache/\*`) when you genuinely intend to tear it down.

---

## Persistence (what survives a VM rebuild)

| State | Location | Durable? |
|---|---|---|
| Run history / task state | Cloud SQL (`airflow` DB) | ✅ |
| App data (datasets, mappings) | Cloud SQL (`dag_data` DB) | ✅ |
| Task logs | GCS (`…-airflow-logs`) | ✅ |
| DAG code | git (re-cloned on boot) | ✅ |
| Secrets | Secret Manager | ✅ |
| **Paper-mapping artifacts** | **local VM volume** | ⚠️ **ephemeral — TODO (separate PR), see README** |

---

## Known follow-ups (not in this deploy)

- **Paper-mapping → GCS** (separate PR): switch the DAGs to `ObjectStoragePath` and
  point `*_PAPER_MAPPING_OUTPUT_DIR` at `gs://neuro-d3-staging-paper-mapping/...`.
  Bucket + IAM already provisioned. See the TODO in README.
- **Airflow TLS:** self-signed today; point a hostname at the static IP and flip the
  Caddyfile to Let's Encrypt (no infra change).
- **CI/Workload Identity Federation:** intentionally deferred.
- **Static frontend build:** optional perf/cost optimization over the dev server.
