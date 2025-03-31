variable "project" {
  description = "Project"
  default     = "western-diorama-455122-u6"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "bq_prod_dataset_name" {
  description = "My Prod BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "weather_prod"
}

variable "bq_stag_dataset_name" {
  description = "My Prod BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "weather_staging"
}

variable "gcs_test_bucket_name" {
  description = "My Test Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "terraform-demo-terra-bucket-hlui-de-zoomcamp"
}
