# Single provider for the staging project. Applies run locally via ADC
# (gcloud auth application-default login). No quota-project / user_project_override
# is needed for app resources.
provider "google" {
  project = var.project_id
  region  = var.region
}
