locals {
  # Artifact Registry base URL: <region>-docker.pkg.dev/<project>/<repo>
  ar_repo_url = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repo_id}"

  # Secret IDs follow CLAUDE.md's d3/<env>/<name> convention, flattened to
  # d3-staging-<name> since GCP secret IDs cannot contain "/".
  secret_ids = {
    openrouter_api_key = "d3-staging-openrouter-api-key"
    airflow_fernet_key = "d3-staging-airflow-fernet-key"
    airflow_jwt_secret = "d3-staging-airflow-jwt-secret"
    airflow_api_secret = "d3-staging-airflow-api-secret-key"
    airflow_admin_pw   = "d3-staging-airflow-admin-password"
    db_master_password = "d3-staging-db-master-password"
  }
}
