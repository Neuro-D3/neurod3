# Minimal app VPC. The project's default VPC was deleted (auto_create_network=false),
# so the Airflow VM needs a network to attach a NIC to. This VPC exists ONLY for
# the VM — Cloud SQL connectivity does not use it (public IP + connector/proxy).
#
# Per CLAUDE.md "No NAT": the VM gets a public IP and uses it for egress
# (image pulls, dataset + OpenRouter APIs). SSH is locked to the IAP range. The
# Airflow UI is fronted by Caddy (TLS) on 80/443; Caddy reverse-proxies to the
# api-server on localhost:8080, so 8080 itself is NOT exposed. Staging uses a
# self-signed cert (Caddy `tls internal`) until a hostname/Let's Encrypt is set up.

resource "google_compute_network" "vpc" {
  name                    = "neuro-d3-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "neuro-d3-subnet-uswest1"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = "10.10.0.0/24"
  private_ip_google_access = true # reach Google APIs without leaving Google's network
}

# SSH (22) reachable ONLY via IAP TCP forwarding.
resource "google_compute_firewall" "iap_ssh" {
  name          = "neuro-d3-allow-iap-ssh"
  network       = google_compute_network.vpc.id
  direction     = "INGRESS"
  source_ranges = ["35.235.240.0/20"] # Google IAP range
  target_tags   = ["airflow"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# Public HTTPS (and HTTP->HTTPS redirect) for the Caddy-fronted Airflow UI.
# Caddy terminates TLS and proxies to the api-server on localhost:8080.
# 8080 is intentionally NOT opened — it's reachable only on the VM itself.
resource "google_compute_firewall" "airflow_web_public" {
  name          = "neuro-d3-allow-airflow-web"
  network       = google_compute_network.vpc.id
  direction     = "INGRESS"
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["airflow"]

  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }
}

# Explicit egress allow (documents intent; default is allow-all).
resource "google_compute_firewall" "egress_all" {
  name               = "neuro-d3-allow-egress"
  network            = google_compute_network.vpc.id
  direction          = "EGRESS"
  destination_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "all"
  }
}
