# Bucket for the paper-mapping DAGs' output: the paper-text-fetcher cache (one
# JSON per DOI), the per-DOI full text the citation phase writes, and run
# artifacts. The Airflow VM mounts it with gcsfuse at /mnt/airflow-output, which
# the containers see as /opt/airflow/output (deploy/gcp/staging/mount-output-bucket.sh).
#
# Nothing in here expires. Cached papers are expensive to refetch (about 7 s
# each, and a third have no open text at all), papers.fulltext_cache_key points
# at these objects, and run artifacts are kept for audit.

resource "google_storage_bucket" "data" {
  name                        = "${var.project_id}-paper-mapping"
  location                    = var.region
  uniform_bucket_level_access = true
  # The cache is the one piece of staging state that is not reproducible for
  # free, so `terraform destroy` must not take it with the rest.
  force_destroy = false
}

# Only the Airflow VM reads/writes the cache (gcsfuse authenticates as the VM
# service account through the metadata server). objectAdmin covers the objects;
# legacyBucketReader adds storage.buckets.get, which gcsfuse and
# `gcloud storage rsync` need to open the bucket at all.
resource "google_storage_bucket_iam_member" "airflow_object_admin" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_storage_bucket_iam_member" "airflow_bucket_reader" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

# Durable Airflow task logs (Airflow native GCS remote logging writes here, and
# the api-server reads them back for the UI). Kept in its own bucket so logs can
# expire while the paper cache above is kept.
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
