# Single GCE VM running the Airflow docker-compose stack (api-server, scheduler,
# dag-processor, triggerer; LocalExecutor). State lives in Cloud SQL so the VM is
# recreatable. Reaches Cloud SQL via the Auth Proxy (startup script), pulls the
# airflow image from Artifact Registry, and reads secrets from Secret Manager.
#
# Static public IP (no NAT) so the public Airflow UI URL is stable across
# stop/start. SSH is IAP-only; the Airflow UI (:8080) is public (see network.tf).

resource "google_compute_address" "airflow" {
  name         = "neuro-d3-airflow-ip"
  region       = var.region
  address_type = "EXTERNAL"
}

resource "google_compute_instance" "airflow" {
  name         = "neuro-d3-airflow"
  machine_type = var.vm_machine_type
  zone         = var.zone
  tags         = ["airflow"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = var.vm_boot_disk_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.id
    access_config {
      nat_ip = google_compute_address.airflow.address # static external IP
    }
  }

  service_account {
    email  = google_service_account.airflow.email
    scopes = ["cloud-platform"] # rely on IAM roles, not legacy scopes
  }

  metadata = {
    enable-oslogin = "TRUE"
    startup-script = templatefile("${path.module}/startup-airflow.sh.tftpl", {
      project_id          = var.project_id
      region              = var.region
      sql_connection_name = google_sql_database_instance.pg.connection_name
      airflow_image       = var.airflow_image
      data_bucket         = google_storage_bucket.data.name
      logs_bucket         = google_storage_bucket.airflow_logs.name
      airflow_base_url    = "https://${google_compute_address.airflow.address}"
    })
  }

  allow_stopping_for_update = true

  # Ensure the proxy can authenticate and secrets are readable before boot.
  depends_on = [
    google_project_iam_member.airflow_cloudsql_client,
    google_project_iam_member.airflow_ar_reader,
    google_secret_manager_secret_iam_member.airflow_access,
    google_sql_user.airflow,
  ]
}
