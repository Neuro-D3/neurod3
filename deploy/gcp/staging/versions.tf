# Terraform + provider pins and remote state.
#
# State lives in the GCS bucket the platform repo (Dura-Labs/duralabs-infra)
# created and published for this project. We do NOT manage that bucket here.
terraform {
  required_version = "~> 1.15.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }

  backend "gcs" {
    bucket = "duralabs-tfstate-projects-neuro-d3-staging"
    prefix = "staging"
  }
}
