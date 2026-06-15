# Bucket for paper-mapping DAG output.
# TODO(separate PR): the DAGs don't write here yet — paper-mapping output is
# currently ephemeral on the VM. A follow-up PR will switch the DAGs to Airflow
# ObjectStoragePath and point *_PAPER_MAPPING_OUTPUT_DIR at
# gs://<this bucket>/<source>_paper_mapping (ADC auth via the VM service account,
# which already has objectAdmin below). The bucket is provisioned now so that PR
# is a pure DAG change.

resource "google_storage_bucket" "data" {
  name                        = "${var.project_id}-paper-mapping"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true # staging convenience

  # Artifacts are reproducible by re-running DAGs; auto-clean to control cost.
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# Only the Airflow VM reads/writes artifacts.
resource "google_storage_bucket_iam_member" "airflow_object_admin" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

# Durable Airflow task logs (Airflow native GCS remote logging writes here, and
# the api-server reads them back for the UI). Kept in its own bucket so logs and
# (future) paper-mapping artifacts have independent lifecycles.
resource "google_storage_bucket" "airflow_logs" {
  name                        = "${var.project_id}-airflow-logs"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_iam_member" "airflow_logs_object_admin" {
  bucket = google_storage_bucket.airflow_logs.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}
