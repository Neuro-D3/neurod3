# Initialize the dag_data schema via gcloud sql import.
# Runs after the Cloud SQL instance is ready; idempotent (repeated imports
# succeed if the schema already exists).
resource "null_resource" "init_schema" {
  provisioner "local-exec" {
    command = <<-EOT
      gcloud storage cp ../../../database/init-db.sql gs://${google_storage_bucket.airflow_logs.name}/init-db.sql --project ${var.project_id} --quiet
      gcloud sql import sql ${google_sql_database_instance.pg.name} gs://${google_storage_bucket.airflow_logs.name}/init-db.sql --database dag_data --project ${var.project_id} --quiet
      gcloud storage rm gs://${google_storage_bucket.airflow_logs.name}/init-db.sql --project ${var.project_id} --quiet
    EOT
  }

  depends_on = [
    google_sql_database_instance.pg,
    google_sql_database.dag_data,
    google_storage_bucket.airflow_logs,
  ]
}
