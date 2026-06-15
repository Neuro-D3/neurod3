# NeuroD3 — staging (GCP) Terraform

Application infrastructure for the **staging** cloud environment, inside the
pre-provisioned project **`neuro-d3-staging`** (`us-west1`).

> **Boundary (Model A).** This module owns only resources *inside* the project.
> The project, folder, billing/budgets, API enablement, and the Terraform state
> bucket are owned by the platform repo (`Dura-Labs/duralabs-infra`) — never
> created or modified here. State lives in
> `gs://duralabs-tfstate-projects-neuro-d3-staging` (prefix `staging`).
>
> The **test/dev** environment is the existing OCI PR-preview stack — not this.

## What this provisions

| Resource | Purpose |
|---|---|
| Artifact Registry (`neuro-d3`) | Docker images: `api`, `frontend`, `airflow` |
| Cloud SQL (Postgres 16, `db-f1-micro`, public IP) | Two DBs: `airflow` + `dag_data`, user `airflow` |
| Secret Manager (`d3-staging-*`) | OpenRouter key, Airflow Fernet/JWT/API/admin secrets, DB password |
| Cloud Run — `neuro-d3-api` | FastAPI service (`:8000`), Cloud SQL connector, scale-to-zero |
| Cloud Run — `neuro-d3-frontend` | React app (`:3000`, dev server), scale-to-zero |
| GCE VM — `neuro-d3-airflow` | Airflow docker-compose stack + Caddy (TLS); DB via Cloud SQL Auth Proxy; static IP |
| VPC + subnet + firewall | VM NIC; SSH IAP-only, Airflow via Caddy `:80/:443` public; **no NAT** |
| GCS bucket (`neuro-d3-staging-paper-mapping`) | Paper-mapping DAG output — **reserved**; not written yet (see TODO) |
| GCS bucket (`…-airflow-logs`) | Airflow task logs (native GCS remote logging) |
| 3 service accounts + scoped IAM | Least-privilege identities for API / frontend / VM |

**Reachability (staging — no `/pr-<N>` prefixes, one stable env):**
- **Frontend** and **API** — each at its own stable Cloud Run `*.run.app` URL;
  `REACT_APP_API_URL` wires the frontend to the API.
- **Airflow UI** — **public** via Caddy at `https://<static-vm-ip>` (the
  `airflow_url` output), protected by Airflow's own login. Caddy terminates TLS
  and proxies to the api-server on `localhost:8080` (8080 is not exposed). Staging
  uses a **self-signed** cert (`tls internal`) → browser warning; swap to a
  hostname + Let's Encrypt later (just point an A record at the static IP and set
  the domain in the Caddyfile).
- **pgAdmin** — **not deployed.** Use Cloud SQL Studio / the cloud console (or
  `gcloud sql connect`) for ad-hoc queries.
- **Postgres** — not a web endpoint; reached only via the Cloud SQL connector
  (API) and Auth Proxy (VM).

Cloud SQL connectivity: **public IP + connector/Auth Proxy** (no VPC peering).
Cloud Run uses the built-in connector; the VM uses the Cloud SQL Auth Proxy. No
`vpcaccess` / private-service-access is used, so no extra APIs are required.

## Prerequisites

- Terraform `~> 1.15`, `gcloud` CLI.
- ADC login: `gcloud auth application-default login`
  - If gcloud errors with *"python not found"* on this machine:
    `$env:CLOUDSDK_PYTHON = "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\platform\bundledpython\python.exe"`
  - ADC tokens expire (`invalid_rapt`) — re-run the login when that happens.
- `cp terraform.tfvars.example terraform.tfvars` and fill it in. Leave
  `openrouter_api_key` / `airflow_fernet_key` as `REPLACE_ME` for the first apply.

## Apply runbook (staged — Cloud Run needs images to exist first)

```powershell
terraform init

# Phase 0 — create the registry (and you can include the rest of the non-image
# resources) before any image exists.
terraform apply -target=google_artifact_registry_repository.neuro_d3

# Phase 1 — build & push the three images. Build/deploy the API first so its URL
# is known before building the frontend (CRA bakes REACT_APP_API_URL at build).
$REPO = "us-west1-docker.pkg.dev/neuro-d3-staging/neuro-d3"
gcloud auth configure-docker us-west1-docker.pkg.dev
docker build -t "$REPO/api:v1"      ./api      ; docker push "$REPO/api:v1"
docker build -t "$REPO/airflow:v1"  ./airflow  ; docker push "$REPO/airflow:v1"
# frontend: see "Frontend API URL" below, then:
docker build -t "$REPO/frontend:v1" ./frontend ; docker push "$REPO/frontend:v1"

# Phase 2 — full apply with the real image refs in terraform.tfvars.
terraform apply

# Post-apply:
#  - add real OPENROUTER_API_KEY + Fernet key as new secret versions
#  - run Alembic migrations to create the dag_data tables
#  - verify the VM: gcloud compute ssh neuro-d3-airflow --zone us-west1-a --tunnel-through-iap
```

Prefer immutable image **digests** over `:tag` in `terraform.tfvars` so each push
triggers a deterministic redeploy.

## Deploy assets & app-code (status)

**Provided in this module / done:**
- **`docker-compose.gce.yml` + `Caddyfile`** — the Airflow-only stack the VM runs:
  a `cloudsql-proxy` sidecar (ADC-authed; Airflow connects to `cloudsql-proxy:5432`),
  the four Airflow services with **FabAuthManager** + admin from
  `d3-staging-airflow-admin-password`, and **Caddy** terminating TLS (`tls internal`,
  self-signed) on 80/443 → `airflow-api-server:8080`. The VM startup script fetches
  secrets, clones the repo, and runs it.
- **API CORS** (`api/main.py`) — now reads an `ALLOWED_ORIGINS` env (comma-separated)
  on top of the local defaults. Set `var.allowed_origins` to the `frontend_url` after
  the first apply and re-apply (kept a variable, not a resource ref, to avoid a cycle).
- **Frontend** — works as-is via the CRA dev server image (see Reachability): the
  `REACT_APP_API_URL` env is read at container start. No prod Dockerfile rewrite needed.

- **Airflow logging** (`requirements.txt`) — `apache-airflow-providers-google` is
  added so the airflow image ships the GCS log handler; the GCE compose enables
  `AIRFLOW__LOGGING__REMOTE_LOGGING` → `gs://…-airflow-logs/airflow-logs` (ADC auth).
**State persistence (what survives a VM rebuild):**
- Run history / task state → **Cloud SQL** metadata DB ✅
- Task logs → **GCS** (`…-airflow-logs`) ✅
- DAG code → git (re-cloned on boot) ✅
- Paper-mapping artifacts → **ephemeral on the VM** ⚠️ (local Airflow volume) — see TODO

### TODO (separate PR): paper-mapping output → GCS
Paper-mapping DAG output is currently written to a local VM volume and is lost on
VM rebuild. The `neuro-d3-staging-paper-mapping` bucket + the VM's `objectAdmin`
IAM are already provisioned for this. The follow-up is a **DAG-only** change: in
each `dags/*_paper_mapping.py`, switch `_get_output_root()` to return Airflow's
`ObjectStoragePath` and change the inline `open(p, …)` write/cache-read sites to
`p.open(…)`, then set `*_PAPER_MAPPING_OUTPUT_DIR` to
`gs://neuro-d3-staging-paper-mapping/<source>_paper_mapping` (in the GCE compose /
startup env). The Google provider is already baked into the airflow image, so no
new dependency is needed. (We held this out of the infra PR since it touches core
pipeline write/cache logic and needs a real DAG run to verify.)

**Still owned by app/ops (not infra):**
- **Schema** — Cloud SQL only creates empty databases. Create the `dag_data` tables
  via Alembic (`init-db.sql` → `0001_initial`) before/at deploy.
- **Static frontend (optional)** — a `npm run build` + nginx image is a later
  cost/perf optimization over the dev server (Oracle also runs the dev server).

## Notes

- `db_deletion_protection = false` and the bucket's `force_destroy = true` so
  `terraform destroy` works for staging.
- The Airflow VM defaults to `e2-small` (2 GB). If Airflow OOMs, bump
  `vm_machine_type = "e2-medium"` (one line) — this matches prod's VM size.
- CI/Workload Identity Federation is intentionally **not** built here yet.
