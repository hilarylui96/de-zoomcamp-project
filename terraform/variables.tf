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

variable "service_account_name" {
  default = "serviceAccount:airflow@western-diorama-455122-u6.iam.gserviceaccount.com"
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
