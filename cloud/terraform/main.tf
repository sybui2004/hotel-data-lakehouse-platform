terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
  required_version = ">= 1.14.0"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "hotel_datalake" {
  name     = "hotel-data-lake"
  location = var.region

  force_destroy               = true
  uniform_bucket_level_access = true
}