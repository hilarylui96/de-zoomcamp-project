# You can update all variables that are necessary for reproducing results 
USER = "hilaryluitszching@gmail.com"
PROJECT_ID = "western-diorama-455122-u6"
LOCATION = "US"
REGION = "us-central1"

BUCKET = "weather-alerts-hlui"
TEMP_BUCKET = "weather-alerts-hlui-temp"
STAG_DATASET_ID = "weather_staging"
PROD_DATASET_ID = "weather_prod"
GCS_PATH = "gs://weather-alerts-hlui/"
CLUSTER_NAME = 'weather-alerts'

CONTAINER_PATH = "/opt/airflow/data"
DEFAULT_ARGS = {
  "gcp_conn_id": "google_cloud_default",
  "project_id": PROJECT_ID,
  "location": LOCATION,
  "bucket": BUCKET,
  "region": REGION
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
    },
    "software_config": {
        "image_version": "2.1-debian11"
    }
}
