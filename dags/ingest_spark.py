import requests
import pandas as pd
from datetime import datetime
import json
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DEFAULT_ARGS = {
  "gcp_conn_id": "google_cloud_default",
  "project_id": "western-diorama-455122-u6",
  "location": "US",
  "bucket":"weather_alerts_hlui",
  "cluster_name": 'weather-alerts',
  "region": "us-central1"
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

PYSPARK_JOB = {
    "reference": {"project_id": "western-diorama-455122-u6"},
    "placement": {"cluster_name": 'weather-alerts'},
    "pyspark_job": {"main_python_file_uri": "gs://weather_alerts_hlui/spark/spark_processing.py"},
}

ingestion_date = datetime.strftime(datetime.today(), '%Y-%m-%d')
file_name = f"{ingestion_date}.json"
file_name_parquet = f"{ingestion_date}.parquet"
inward_path = "/data"
outward_path = "/opt/airflow/data"

def ingest():
  endpoint = 'https://api.weather.gov/alerts'
  headers = {'User-Agent' : 'hilaryluitszching@gmail.com'}
  response = requests.get(endpoint, headers=headers)
  data = response.json()
  with open(f'{outward_path}/raw_json/{file_name}', "w") as f:
    json.dump(data.get('features',[]), f)

with DAG (
  dag_id="ingest_weather_alerts",
  start_date=datetime(2025,3,27),
  schedule='@daily',
  catchup=True,
  default_args=DEFAULT_ARGS
) as dag:
  
  ingest_data = PythonOperator(
    task_id="ingest_from_api",
    python_callable=ingest
  )

  upload_to_gcs_task = LocalFilesystemToGCSOperator(
     task_id="upload_to_gcs",
     src=f"{outward_path}/raw_json/{file_name}",
     dst="raw_data/"
  )

  upload_script = LocalFilesystemToGCSOperator(
      task_id="upload_pyspark_script",
      src="scripts/spark_processing.py",
      dst="spark/spark_processing.py"
  )

  create_cluster = DataprocCreateClusterOperator(
      task_id="create_cluster",
      cluster_config=CLUSTER_CONFIG
  )

  submit_job = DataprocSubmitJobOperator(
      task_id="submit_pyspark_job",
      job=PYSPARK_JOB
    )
  
  # delete_cluster = DataprocDeleteClusterOperator(
  #       task_id="delete_cluster",
  #       trigger_rule="all_done"  # Ensures cluster is deleted even if job fails
  #   )
  
  # create_temp_bq_table = GCSToBigQueryOperator(
  #       task_id='load_parquet_to_bq',
  #       source_objects=[f'parquet/{ingestion_date}/*'],
  #       destination_project_dataset_table=f'western-diorama-455122-u6.weather_staging.alerts_{ingestion_date}',
  #       source_format='PARQUET',
  #       write_disposition='WRITE_TRUNCATE',
  #       autodetect=True
  #   )


ingest_data >> upload_to_gcs_task >> [upload_script, create_cluster] >> submit_job