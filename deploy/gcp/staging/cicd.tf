# CI/CD identity for GitHub Actions → staging.
#
# GitHub Actions authenticates with GCP via Workload Identity Federation (keyless
# OIDC — no service-account JSON key) and impersonates a least-privilege deployer
# SA that can: push images to Artifact Registry, deploy Cloud Run revisions, and
# SSH the Airflow VM over IAP (OS Login is already enabled on the VM).
#
# Bootstrap (one-time, run locally with project IAM-admin rights):
#   terraform apply
#   terraform output -raw cicd_wif_provider   # -> GitHub repo variable GCP_WIF_PROVIDER
#   terraform output -raw cicd_deploy_sa       # -> GitHub repo variable GCP_DEPLOY_SA
#
# Rollout: keep var.wif_lock_to_main = false while testing on PRs (any ref in the
# repo may deploy — PR runs present ref refs/pull/N/merge). Flip it to true at
# go-live so only refs/heads/main can deploy, then `terraform apply`.

# ─── Workload Identity Pool + GitHub OIDC provider ──────────────────────────
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"
  description               = "OIDC federation for GitHub Actions staging deploys."
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  # Pin the provider to our repo, independent of the principalSet binding below.
  attribute_condition = "assertion.repository == '${var.github_repo}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# ─── Deployer service account ───────────────────────────────────────────────
resource "google_service_account" "deployer" {
  account_id   = "gh-actions-deployer"
  display_name = "GitHub Actions deployer (staging CI/CD)"
}

locals {
  # Phase 1 (testing): any ref in the repo may impersonate the deployer SA.
  # Phase 2 (go-live): only refs/heads/main (the repo is already pinned by the
  # provider attribute_condition above, so ref alone is sufficient).
  wif_member = (
    var.wif_lock_to_main
    ? "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.ref/refs/heads/main"
    : "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
  )
}

resource "google_service_account_iam_member" "deployer_wif" {
  service_account_id = google_service_account.deployer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = local.wif_member
}

# ─── Deployer project roles (least privilege) ───────────────────────────────
# Push images to Artifact Registry.
resource "google_project_iam_member" "deployer_ar_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.deployer.email}"
}

# Deploy new Cloud Run revisions (image-only `gcloud run services update`).
resource "google_project_iam_member" "deployer_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.deployer.email}"
}

# IAP TCP tunnel to reach VM SSH (port 22 is locked to the IAP range).
resource "google_project_iam_member" "deployer_iap_tunnel" {
  project = var.project_id
  role    = "roles/iap.tunnelResourceAccessor"
  member  = "serviceAccount:${google_service_account.deployer.email}"
}

# OS Login with sudo, scoped to the Airflow VM ONLY (instance-level binding) so a
# leaked token can't gain root on other project VMs. Runs airflow-compose.sh via sudo.
resource "google_compute_instance_iam_member" "deployer_os_admin_login" {
  project       = var.project_id
  zone          = var.zone
  instance_name = google_compute_instance.airflow.name
  role          = "roles/compute.osAdminLogin"
  member        = "serviceAccount:${google_service_account.deployer.email}"
}

# ─── actAs the runtime / VM service accounts ────────────────────────────────
# Deploying a Cloud Run revision runs as the service's runtime SA, and SSHing an
# instance that has a SA both require serviceAccountUser on that SA.
resource "google_service_account_iam_member" "deployer_act_as_api" {
  service_account_id = google_service_account.api.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}

resource "google_service_account_iam_member" "deployer_act_as_frontend" {
  service_account_id = google_service_account.frontend.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}

resource "google_service_account_iam_member" "deployer_act_as_airflow" {
  service_account_id = google_service_account.airflow.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}

# ─── Outputs → set these as GitHub repository variables ─────────────────────
output "cicd_wif_provider" {
  description = "Full WIF provider resource name -> GitHub repo variable GCP_WIF_PROVIDER."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "cicd_deploy_sa" {
  description = "Deployer SA email -> GitHub repo variable GCP_DEPLOY_SA."
  value       = google_service_account.deployer.email
}
