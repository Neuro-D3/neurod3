# Dedicated runtime identities for each workload (least privilege).
# We never run app workloads as the default Compute/Run service account.
#
# Secret access is granted per-secret in secrets.tf; bucket access on the
# bucket in storage.tf. Only genuinely project-scoped roles live here.

resource "google_service_account" "api" {
  account_id   = "neuro-d3-api"
  display_name = "NeuroD3 API (Cloud Run)"
}

resource "google_service_account" "frontend" {
  account_id   = "neuro-d3-frontend"
  display_name = "NeuroD3 Frontend (Cloud Run)"
}

resource "google_service_account" "airflow" {
  account_id   = "neuro-d3-airflow"
  display_name = "NeuroD3 Airflow (GCE VM)"
}

# ─── API: connect to Cloud SQL via the Cloud Run connector ──────────────────
resource "google_project_iam_member" "api_cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.api.email}"
}

# ─── Airflow VM ─────────────────────────────────────────────────────────────
# cloudsql.client  : authenticate the Cloud SQL Auth Proxy
# logging/monitoring: VM ops + container logs/metrics
# artifactregistry.reader: docker pull the airflow image
resource "google_project_iam_member" "airflow_cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_ar_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# Frontend SA intentionally has no project-level roles (static server, no GCP access).
