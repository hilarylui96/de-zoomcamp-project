terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
  backend "gcs" {
    bucket  = "terraform-state-hlui"   # <- this bucket must already exist
    prefix  = "env/prod"             # <- folder path inside the bucket
  }
}

provider "google" {
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = false
}

resource "google_storage_bucket" "temp-bucket" {
  name          = var.gcs_temp_bucket_name
  location      = var.location
  force_destroy = false
}

resource "google_bigquery_dataset" "prod_dataset" {
  dataset_id = var.bq_prod_dataset_name
  location   = var.location
}

resource "google_bigquery_dataset" "stag_dataset" {
  dataset_id = var.bq_stag_dataset_name
  location   = var.location
}

resource "google_project_iam_member" "terraform_sa_roles" {
  for_each = toset([
  "roles/bigquery.admin",
  "roles/editor",
  "roles/resourcemanager.projectIamAdmin",
  "roles/iam.serviceAccountAdmin",
  "roles/iam.securityAdmin"
  ])
  project = var.project
  role    = each.value
  member  = var.terraform_sa
}

resource "google_project_iam_member" "airflow_sa_roles" {
  for_each = toset([
  "roles/storage.admin",
  "roles/bigquery.admin",
  "roles/editor",
  "roles/dataproc.admin",
  ])
  project = var.project
  role    = each.value
  member  = var.airflow_sa
}