# Durable bucket for the find_reuse cache: paper full text, per-archive
# classification and dataset snapshots, lookup metadata, and the seed manifest
# (see the seeding epic, Neuro-D3/neurod3#81, for the layout and seeding flow).
#
# This is deliberately NOT the paper-mapping bucket above it in storage.tf.
# That bucket auto-deletes objects after 30 days on the premise that its
# contents are reproducible by re-running DAGs. The find_reuse cache is the
# opposite case: it holds multi-source full-text fetches and tens of thousands
# of LLM classifications that cost weeks and tokens to regenerate. So this
# bucket has versioning, no lifecycle expiry, and no force_destroy.

resource "google_storage_bucket" "find_reuse_cache" {
  name                        = "${var.project_id}-find-reuse-cache"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  # Protects against bad uploads and manifest.json regeneration mistakes.
  # Snapshot files (classifications/, datasets/, manifest.json) are replaced
  # wholesale on each cache refresh; versioning retains the prior copies.
  versioning {
    enabled = true
  }

  # Keep version storage bounded without ever expiring live objects: retain the
  # two most recent noncurrent versions and delete older ones. num_newer_versions
  # counts the live version, so 3 newer versions means 2 archived are kept plus
  # the live object.
  lifecycle_rule {
    condition {
      num_newer_versions = 3
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
}

# The Airflow VM reads the cache (seed, classification DAGs) and uploads newly
# fetched papers (paper-mapping DAGs). Auth is ADC via the VM service account.
# The API and frontend get no access; full text is never served.
# Maintainer laptop uploads use the maintainer's own gcloud identity
# (docs/gcp_setup.md).
resource "google_storage_bucket_iam_member" "find_reuse_cache_airflow_object_admin" {
  bucket = google_storage_bucket.find_reuse_cache.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}
