variable "project" {
  description = "Project"
  default     = "western-diorama-455122-u6"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "airflow_sa" {
  default = "serviceAccount: airflow@western-diorama-455122-u6.iam.gserviceaccount.com"
}

variable "terraform_sa" {
  default = "serviceAccount:terraform@western-diorama-455122-u6.iam.gserviceaccount.com"
}

variable "bq_prod_dataset_name" {
  description = "My Prod BigQuery Dataset Name"
  default     = "weather_prod"
}

variable "bq_stag_dataset_name" {
  description = "My Prod BigQuery Dataset Name"
  default     = "weather_staging"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "weather-alerts-hlui"
}

variable "gcs_temp_bucket_name" {
  description = "My Temp Storage Bucket Name"
  default     = "weather-alerts-hlui-temp"
}

variable "terraform_state_bucket_name" {
  description = "My Terraform State Bucket Name"
  default     = "terraform-state-hlui"
}
