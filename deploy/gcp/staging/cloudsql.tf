# Cloud SQL Postgres 16: one instance, two databases (airflow + dag_data),
# user "airflow". Matches the local docker-compose / init-db.sql layout.
#
# Networking: PUBLIC IP, reached via the Cloud SQL connector (Cloud Run API)
# and the Cloud SQL Auth Proxy (VM) — both IAM-gated by roles/cloudsql.client.
# No private service access / VPC peering, no authorized_networks (no IP allowlist).

resource "google_sql_database_instance" "pg" {
  name                = "neuro-d3-pg"
  database_version    = "POSTGRES_16"
  region              = var.region
  deletion_protection = var.db_deletion_protection

  settings {
    tier              = var.db_tier
    availability_type = "ZONAL"
    disk_autoresize   = true
    disk_size         = 10

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
    }

    ip_configuration {
      ipv4_enabled = true
      # No private_network and no authorized_networks: all access is via the
      # Cloud SQL connector / Auth Proxy, not by IP.
    }
  }
}

resource "google_sql_database" "airflow" {
  name     = "airflow"
  instance = google_sql_database_instance.pg.name
}

resource "google_sql_database" "dag_data" {
  name     = "dag_data"
  instance = google_sql_database_instance.pg.name
}

# Single DB user shared by Airflow's SQLAlchemy conn and the API's DB_USER.
# Password is the single source of truth in secrets.tf (random_password.db),
# also stored as the db-master-password secret version.
resource "google_sql_user" "airflow" {
  name     = "airflow"
  instance = google_sql_database_instance.pg.name
  password = random_password.db.result
}
