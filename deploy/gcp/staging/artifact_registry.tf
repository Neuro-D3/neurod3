# Single Docker repo holding the three app images: api, frontend, airflow.
# Image paths: ${local.ar_repo_url}/{api,frontend,airflow}:<tag>
#
# The artifactregistry API is already enabled on the project by the platform repo.
resource "google_artifact_registry_repository" "neuro_d3" {
  location      = var.region
  repository_id = var.ar_repo_id
  format        = "DOCKER"
  description   = "NeuroD3 app images (api, frontend, airflow) for staging"
}
