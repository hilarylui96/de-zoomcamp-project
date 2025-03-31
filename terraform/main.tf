resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_test_bucket_name
  location      = var.location
  force_destroy = false
}
