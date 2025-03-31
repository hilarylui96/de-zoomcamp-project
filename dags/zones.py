import requests
import pandas as pd
from datetime import datetime
import json
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from utilties.constants import (
  DEFAULT_ARGS, CLUSTER_CONFIG, CONTAINER_PATH, GCS_PATH, USER, 
  CLUSTER_NAME, PROD_DATASET_ID, TEMP_BUCKET
)

def ingest_zone():
  endpoint = 'https://api.weather.gov/zones'
  headers = {'User-Agent' : USER}
  response = requests.get(endpoint, headers=headers)
  data = response.json()
  with open(f'{CONTAINER_PATH}/zones.json', "w") as f:
    json.dump(data.get('features',[]), f)

with DAG (
  dag_id="ingest_zones",
  start_date=datetime(2025,3,27),
  schedule='@once',
  catchup=True,
  default_args=DEFAULT_ARGS
) as dag2:
    
  ingest_data = PythonOperator(
    task_id="ingest_from_api",
    python_callable=ingest_zone

  )
  upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id="upload_to_gcs",
    src=f"{CONTAINER_PATH}/zones.json",
    dst="raw_data/"
  )

  upload_script = LocalFilesystemToGCSOperator(
      task_id="upload_pyspark_script",
      src="scripts/zones.py",
      dst="spark/zones.py"
  )

  create_cluster = DataprocCreateClusterOperator(
      task_id="create_cluster",
      cluster_name= CLUSTER_NAME,
      use_if_exists=True,
      cluster_config=CLUSTER_CONFIG
  )

  submit_job = DataprocSubmitJobOperator(
      task_id="submit_pyspark_job",
      job= {
        "reference": {"project_id": DEFAULT_ARGS['project_id']},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
          "main_python_file_uri": f"{GCS_PATH}/spark/zones.py",
          "args": [GCS_PATH, 
                  DEFAULT_ARGS['project_id'], 
                  PROD_DATASET_ID,
                  TEMP_BUCKET
                  ]
        }
     } 
  )
  
  delete_cluster = DataprocDeleteClusterOperator(
      task_id="delete_cluster",
      cluster_name= CLUSTER_NAME
  )

  purge_file_task = BashOperator(
     task_id="purge_file",
     bash_command=f"rm -rf zones.json",
     cwd= CONTAINER_PATH
  )
    
  ingest_data >> upload_to_gcs_task
  [upload_script, upload_to_gcs_task, create_cluster] >> submit_job
  submit_job >> [delete_cluster, purge_file_task]
  
  