# ─── Project / location (platform-provided facts) ───────────────────────────
variable "project_id" {
  description = "GCP project ID for the staging environment."
  type        = string
  default     = "neuro-d3-staging"
}

variable "project_number" {
  description = "GCP project number (used where a member string needs the number)."
  type        = string
  default     = "601000536186"
}

variable "region" {
  description = "Default region for all regional resources."
  type        = string
  default     = "us-west1"
}

variable "zone" {
  description = "Zone for the Airflow GCE VM."
  type        = string
  default     = "us-west1-a"
}

# ─── Artifact Registry / images ─────────────────────────────────────────────
variable "ar_repo_id" {
  description = "Artifact Registry Docker repository ID."
  type        = string
  default     = "neuro-d3"
}

variable "api_image" {
  description = "Full image ref for the API Cloud Run service (prefer a digest). e.g. us-west1-docker.pkg.dev/neuro-d3-staging/neuro-d3/api:<tag>"
  type        = string
}

variable "frontend_image" {
  description = "Full image ref for the frontend Cloud Run service (prefer a digest)."
  type        = string
}

variable "airflow_image" {
  description = "Full image ref for the Airflow image the VM pulls (prefer a digest)."
  type        = string
}

# ─── Cloud SQL ──────────────────────────────────────────────────────────────
variable "db_tier" {
  description = "Cloud SQL machine tier. Staging default is the smallest shared-core tier."
  type        = string
  default     = "db-f1-micro"
}

variable "db_deletion_protection" {
  description = "Cloud SQL deletion protection. False for staging so destroy works."
  type        = bool
  default     = false
}

# ─── Airflow VM ─────────────────────────────────────────────────────────────
variable "vm_machine_type" {
  description = "GCE machine type for the Airflow VM. e2-medium is the staging default (matches prod's VM size); e2-small OOMs Airflow."
  type        = string
  default     = "e2-medium"
}

variable "vm_boot_disk_gb" {
  description = "Boot disk size (GB) for the Airflow VM."
  type        = number
  default     = 30
}

# ─── Cloud Run sizing ───────────────────────────────────────────────────────
variable "cloudrun_cpu" {
  description = "vCPU per Cloud Run instance (staging: small)."
  type        = string
  default     = "0.25"
}

variable "cloudrun_memory" {
  description = "Memory per Cloud Run instance."
  type        = string
  default     = "512Mi"
}

variable "cloudrun_max_instances" {
  description = "Max Cloud Run instances (staging: single instance, no real autoscale)."
  type        = number
  default     = 1
}

variable "allow_unauthenticated" {
  description = "Allow public (allUsers) invocation of the Cloud Run services."
  type        = bool
  default     = true
}

variable "allowed_origins" {
  description = "Extra CORS origins for the API (comma-separated), e.g. the frontend's Cloud Run URL. Set this after the first apply (once the frontend URL is known) to avoid a TF dependency cycle, then re-apply."
  type        = string
  default     = ""
}

# ─── Secrets (user-supplied values) ─────────────────────────────────────────
variable "openrouter_api_key" {
  description = "OpenRouter API key. Leave as the placeholder for the first apply, then add the real value as a new secret version."
  type        = string
  default     = "REPLACE_ME"
  sensitive   = true
}

variable "airflow_fernet_key" {
  description = "Airflow Fernet key. Must be a real key from cryptography.fernet.Fernet.generate_key(); placeholder is rejected by Airflow at runtime, so set the real value before the VM boots Airflow."
  type        = string
  default     = "REPLACE_ME"
  sensitive   = true
}

# ─── CI/CD (GitHub Actions → Workload Identity Federation) ───────────────────
variable "github_repo" {
  description = "GitHub repository (owner/name) allowed to impersonate the deployer SA via Workload Identity Federation."
  type        = string
  default     = "Neuro-D3/neurod3"
}

variable "wif_lock_to_main" {
  description = "Rollout toggle. false (Phase 1): any ref in github_repo may deploy — required for PR-trigger testing (PR runs present ref refs/pull/N/merge). true (Phase 2): only refs/heads/main may deploy."
  type        = bool
  default     = false
}
