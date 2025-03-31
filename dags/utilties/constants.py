CONTAINER_PATH = "/opt/airflow/data"
USER = 'hilaryluitszching@gmail.com'
PROJECT_ID = "western-diorama-455122-u6"
LOCATION = "US"
BUCKET = "weather_alerts_hlui"
TEMP_BUCKET = "weather_alerts_hlui_temp"
REGION = "us-central1"
STAG_DATASET_ID = "weather_staging"
PROD_DATASET_ID = "weather_prod"
GCS_PATH = "gs://weather_alerts_hlui/"
CLUSTER_NAME = 'weather-alerts'

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
