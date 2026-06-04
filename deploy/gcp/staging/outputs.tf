output "api_url" {
  description = "Public URL of the API Cloud Run service."
  value       = google_cloud_run_v2_service.api.uri
}

output "frontend_url" {
  description = "Public URL of the frontend Cloud Run service."
  value       = google_cloud_run_v2_service.frontend.uri
}

output "sql_connection_name" {
  description = "Cloud SQL instance connection name (project:region:instance)."
  value       = google_sql_database_instance.pg.connection_name
}

output "sql_public_ip" {
  description = "Public IP of the Cloud SQL instance."
  value       = google_sql_database_instance.pg.public_ip_address
}

output "ar_repo_url" {
  description = "Artifact Registry base URL for pushing images."
  value       = local.ar_repo_url
}

output "airflow_vm_ip" {
  description = "Static external IP of the Airflow VM."
  value       = google_compute_address.airflow.address
}

output "airflow_url" {
  description = "Public Airflow UI via Caddy (HTTPS, self-signed cert -> browser warning until a domain/Let's Encrypt is set up)."
  value       = "https://${google_compute_address.airflow.address}"
}

output "airflow_ssh_hint" {
  description = "SSH to the VM (IAP tunnel; SSH is not public)."
  value       = "gcloud compute ssh neuro-d3-airflow --zone ${var.zone} --tunnel-through-iap"
}
