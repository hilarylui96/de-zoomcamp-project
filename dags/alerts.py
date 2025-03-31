import requests
import pandas as pd
from datetime import datetime
import json
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from utilties.constants import (
  DEFAULT_ARGS, CLUSTER_CONFIG, CONTAINER_PATH, GCS_PATH, USER, CLUSTER_NAME, 
  PROJECT_ID, STAG_DATASET_ID, PROD_DATASET_ID, TEMP_BUCKET
)
from utilties.queries import CREATE_PROD_TABLE

def ingest(start_date,end_date):
  start_date_str = f'{start_date}T00:00:00Z'
  end_date_str = f'{end_date}T00:00:00Z'
  endpoint = f'https://api.weather.gov/alerts?start={start_date_str}&end={end_date_str}'
  headers = {'User-Agent' : USER}
  response = requests.get(endpoint, headers=headers)
  data = response.json()
  with open(f'{CONTAINER_PATH}/raw_json/{start_date}.json', "w") as f:
    json.dump(data.get('features',[]), f)

with DAG (
  dag_id="ingest_weather_alerts",
  start_date=datetime(2025,3,30),
  schedule='@daily',
  max_active_runs=1, 
  catchup=True,
  default_args=DEFAULT_ARGS
) as dag:
  
  # Note: These will only get parsed inside tasks 
  # Does not work if you do something like file_name = f"{exuection_date}.json" here. Have to do it inside the tasks
  execution_date = "{{ execution_date.strftime('%Y-%m-%d') }}"
  end_date = "{{ (execution_date + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
  
  ingest_data = PythonOperator(
    task_id="ingest_from_api",
    python_callable=ingest,
     op_kwargs={"start_date": execution_date,
                "end_date": end_date
                }
  )

  upload_to_gcs_task = LocalFilesystemToGCSOperator(
     task_id="upload_to_gcs",
     src=f'{CONTAINER_PATH}/raw_json/{execution_date}.json',
     dst="raw_data/"
  )

  upload_script = LocalFilesystemToGCSOperator(
      task_id="upload_pyspark_script",
      src="scripts/spark_processing.py",
      dst="spark/spark_processing.py"
  )

  create_cluster = DataprocCreateClusterOperator(
      task_id="create_cluster",
      cluster_name=CLUSTER_NAME,
      use_if_exists=True,
      cluster_config=CLUSTER_CONFIG
  )

  create_prod_table = BigQueryInsertJobOperator(
    task_id="create_prod_table",
    configuration={
      "query": {
        "query": CREATE_PROD_TABLE.format(
            project_id=PROJECT_ID,
            dataset_id=PROD_DATASET_ID
        ),
        "useLegacySql": False
      }
    }
  )

  submit_job = DataprocSubmitJobOperator(
      task_id="submit_pyspark_job",
      job={
        "reference": {"project_id": DEFAULT_ARGS['project_id']},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
          "main_python_file_uri": f"{GCS_PATH}/spark/spark_processing.py",
          "args": [execution_date, 
                  GCS_PATH, 
                  PROJECT_ID, 
                  STAG_DATASET_ID,
                  PROD_DATASET_ID,
                  TEMP_BUCKET
                ]
        }
      }
  )
  
  delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=CLUSTER_NAME
    )
  
  purge_file_task = BashOperator(
     task_id="purge_file",
     bash_command=f"rm -rf raw_json/{execution_date}.json",
     cwd= CONTAINER_PATH
  )
  

  ingest_data >> upload_to_gcs_task
  [upload_script, upload_to_gcs_task, create_cluster, create_prod_table] >> submit_job
  submit_job >> [delete_cluster, purge_file_task]
