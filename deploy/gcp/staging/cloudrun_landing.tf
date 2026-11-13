# Landing page for datareuse.org on Cloud Run v2, and the custom domains.
#
# The page is static (landing/: index.html served by nginx). It scales to zero
# and bills per request, so it costs next to nothing at placeholder traffic.
#
# First image: a Cloud Run service can't be created without an image, and CI
# only builds landing/ after this exists, so var.landing_image defaults to
# Google's public sample image. The Deploy to Staging workflow then rolls out the
# real image; ignore_changes keeps Terraform from rolling it back.
#
# Domains (DNS lives in Cloudflare, owned by CatalystNeuro; records are DNS only,
# grey cloud, so Google can issue and renew the certificates):
#   neuro.datareuse.org -> frontend   CNAME neuro -> ghs.googlehosted.com
#   datareuse.org       -> landing    A/AAAA @ -> output landing_dns_records
# Creating a mapping needs the applying account to be a verified owner of
# datareuse.org in Google Search Console.

resource "google_service_account" "landing" {
  account_id   = "neuro-d3-landing"
  display_name = "DataReuse landing page (Cloud Run)"
}

resource "google_cloud_run_v2_service" "landing" {
  name     = "neuro-d3-landing"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.landing.email

    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }

    containers {
      image = var.landing_image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "256Mi"
        }
        # Request-based billing: no CPU charge while idle.
        cpu_idle = true
      }
    }
  }

  # Same as the API and frontend: CI rolls images with `gcloud run services update`.
  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
      client,
      client_version,
    ]
  }
}

resource "google_cloud_run_v2_service_iam_member" "landing_public" {
  count    = var.allow_unauthenticated ? 1 : 0
  name     = google_cloud_run_v2_service.landing.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# CI deploys new revisions that run as the landing service account.
resource "google_service_account_iam_member" "deployer_act_as_landing" {
  service_account_id = google_service_account.landing.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}

# ─── Custom domains ─────────────────────────────────────────────────────────

# Created by hand with gcloud on 2026-11-13; adopted into Terraform unchanged.
import {
  to = google_cloud_run_domain_mapping.neuro
  id = "locations/${var.region}/namespaces/${var.project_id}/domainmappings/neuro.datareuse.org"
}

resource "google_cloud_run_domain_mapping" "neuro" {
  location = var.region
  name     = "neuro.datareuse.org"

  metadata {
    namespace = var.project_id
  }

  spec {
    route_name = google_cloud_run_v2_service.frontend.name
  }

  # gcloud created it without recording certificate_mode, and the provider's
  # default ("AUTOMATIC", which is what it already uses) forces a replacement,
  # which would delete the mapping and re-issue its certificate.
  lifecycle {
    ignore_changes = [spec[0].certificate_mode]
  }
}

resource "google_cloud_run_domain_mapping" "landing" {
  location = var.region
  name     = "datareuse.org"

  metadata {
    namespace = var.project_id
  }

  spec {
    route_name = google_cloud_run_v2_service.landing.name
  }
}
