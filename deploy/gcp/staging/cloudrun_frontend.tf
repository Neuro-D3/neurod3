# React frontend on Cloud Run v2.
#
# Mirrors the Oracle/PR-preview setup: the image runs the CRA dev server
# (`npm start`, frontend/Dockerfile as-is). CRA inlines REACT_APP_* when webpack
# compiles, and for the dev server that compile happens at container START — so
# the REACT_APP_API_URL env below is read at boot and points the app at the API
# service. No build-time bake / prod Dockerfile rewrite is required to get this
# working (a static prod build is a later optimization, not a blocker).
#
# Port: react-scripts honors $PORT (Cloud Run injects it = container_port) and the
# image already sets HOST=0.0.0.0. We keep 3000 to match the Dockerfile's EXPOSE.
#
# We wire ONLY frontend -> api.uri (no cycle; the API's CORS origin is an app-code
# change handled separately). startup_cpu_boost + 1Gi give the dev-server compile
# enough headroom to bind the port before Cloud Run's startup deadline.

resource "google_cloud_run_v2_service" "frontend" {
  name     = "neuro-d3-frontend"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.frontend.email

    scaling {
      min_instance_count = 1
      max_instance_count = var.cloudrun_max_instances
    }

    containers {
      image = var.frontend_image

      ports {
        container_port = 3000
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
        startup_cpu_boost = true
      }

      # Read at container start by the CRA dev server (see header note).
      env {
        name  = "REACT_APP_API_URL"
        value = google_cloud_run_v2_service.api.uri
      }

      # webpack-dev-server rejects unexpected Host headers ("Invalid Host header")
      # when served via a proxy/different hostname like *.run.app. Disable that check.
      env {
        name  = "DANGEROUSLY_DISABLE_HOST_CHECK"
        value = "true"
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "frontend_public" {
  count    = var.allow_unauthenticated ? 1 : 0
  name     = google_cloud_run_v2_service.frontend.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}
