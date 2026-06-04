# FastAPI API on Cloud Run v2. Stateless, reads dag_data over the Cloud SQL
# connector (Unix socket at /cloudsql/<connection_name>). DB password injected
# from Secret Manager; other DB_* values are plain env.

resource "google_cloud_run_v2_service" "api" {
  name     = "neuro-d3-api"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.api.email

    scaling {
      min_instance_count = 0 # scale to zero (staging cost)
      max_instance_count = var.cloudrun_max_instances
    }

    containers {
      image = var.api_image

      ports {
        container_port = 8000
      }

      resources {
        limits = {
          cpu    = "1"
          memory = var.cloudrun_memory
        }
        # Cloud SQL connector volume forces always-allocated CPU mode, which
        # requires >= 1 CPU. cpu_idle = true re-enables throttling at idle.
        cpu_idle = true
      }

      # psycopg treats a host that starts with "/" as a Unix socket directory.
      env {
        name  = "DB_HOST"
        value = "/cloudsql/${google_sql_database_instance.pg.connection_name}"
      }
      env {
        name  = "DB_PORT"
        value = "5432"
      }
      env {
        name  = "DB_NAME"
        value = "dag_data"
      }
      env {
        name  = "DB_USER"
        value = "airflow"
      }
      # Frontend origin(s) for CORS. Empty until you set var.allowed_origins to
      # the frontend's Cloud Run URL (after the first apply).
      env {
        name  = "ALLOWED_ORIGINS"
        value = var.allowed_origins
      }
      env {
        name = "DB_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.this["db_master_password"].secret_id
            version = "latest"
          }
        }
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.pg.connection_name]
      }
    }
  }

  # Permissions must exist before the first revision starts.
  depends_on = [
    google_project_iam_member.api_cloudsql_client,
    google_secret_manager_secret_iam_member.api_db_password,
  ]
}

# Public access for staging.
resource "google_cloud_run_v2_service_iam_member" "api_public" {
  count    = var.allow_unauthenticated ? 1 : 0
  name     = google_cloud_run_v2_service.api.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}
