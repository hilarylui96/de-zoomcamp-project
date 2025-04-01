## Problem
This projects aims to develop an end-to-end data solution that performs Extract, Transform, and Load (ETL) on weather alerts from the National Weather Service (NWS) API to generate meaningful insights. The pipeline ingests raw JSON alert data, stages it, transforms it into an analytics-friendly format, and surfaces key metrics.

Examples questions addressed include: 
* Which zones have received the most alerts in the past x days?
* What is the distribution of alert severity across all events?
* How long do alerts typically remain active?

More details of the API can be found [here](https://www.weather.gov/documentation/services-web-api).

## Solution
### Architecture Diagram

![Flowcharts (3)](https://github.com/user-attachments/assets/d5211c18-5aeb-4823-b503-b5136c3e48cd)

The tools and technologies used in this project are: 
* Cloud: Google Cloud Platform (GCP)
  * Managed Processing Cluster: Dataproc
  * Data Lake: Cloud Storage - Buckets
  * Data Warehouse: BigQuery
  * Data Visualization: Data Studio
* Containerization: Docker, Docker Compose
* Orchestration: Apache Airflow
* Data Transformation: Apache Spark (Pyspark)
* laC: Terraform
* Scripting Language: Python

### ☁️ Weather Alerts Data Pipeline
The data pipeline is orchestrated by Apache Airflow and follows an ELT (Extract, Load, Transform) pattern:
![Screenshot 2025-03-31 at 5 50 52 PM](https://github.com/user-attachments/assets/1984bd12-1133-4301-9900-5afb90163928)

#### Extract
Initial data extraction is handled by a dockerized Apache Airflow instance running locally on a macOS host. Weather alerts are pulled from the NWS API and saved as raw JSON files. These files are then uploaded to a Google Cloud Storage (GCS) bucket for downstream processing.
#### Transform
The transformation step is performed using PySpark on Google Cloud Dataproc. A Python script stored in GCS runs in a Dataproc job to process the raw data. Transformations include:
* Flattening nested JSON fields
* Renaming columns for clarity and consistency
* Removing unnecessary fields
* Adding a unique row identifier for deduplication
#### Load 
The cleaned and transformed data is loaded into two BigQuery datasets: one for staging and one for production. The staging dataset is overwritten on each run, while only new rows (deduplicated using the unique ID) are appended to the production dataset. This enables safe experimentation and reliable incremental loading.

## Partitioning and Clustering 
The final production table is:
* Partitioned on `effective` (a timestamp field that marks when the alert becomes active)
* Clustered by `affectedZone`

This design was chosen to:
* Optimize queries filtering alerts within a recent time window (e.g. `WHERE effective > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)`)
* Improve performance and cost efficiency for queries filtered or grouped by affectedZone, which has high cardinality and is frequently accessed

## Dashboard

![Screenshot 2025-03-31 at 10 10 24 PM](https://github.com/user-attachments/assets/46d5a69d-e1ea-4ac0-87db-fc05375e7f0e)
[Weather Alerts Dashboard (DE Zoomcamp Project)](https://lookerstudio.google.com/s/tWkdtYZvucQ)

## How to reproduce this project?
1. Clone this repo `git clone https://github.com/hilarylui96/de-zoomcamp-project.git`
2. Create a GCP project
3. Create a service account in the project and grant the following permissions:
    * BigQuery Admin
    * Project IAM Admin
    * Security Admin
    * Service Account Admin
    * Storage Admin
4. Generate a JSON credential key
    * store it in $HOME/.google/credentials.json (need to create a new folder called .google first `mkdir .google`)
    * upload it to Github repo as a repository secret and name it as `GOOGLE_CREDENTIALS`
5. Create a bucket in GCS for your terraform state file and name it as `terraform-state-hlui`
6. Update variables
    * `terraform/variables.tf`: update the values for `project`, `region`, `location`, `service_account_name` according to your GCS project
    * `dags/utilities/constants`: update `USER` to you email address, `PROJECT_ID`, `LOCATION`, `REGION` according to your GCS project
7. Push the changes to your repo by `git add . && git push`and Terraform will automatically create the necessary storage buckets and datasets in GCS for you
8. Run `mkdir data` on your machine to create a temp folder for storing the extracted data from API 
9. Run `docker-compose up -d` to intialize Airflow in your container
10. Open `http://localhost:8080/` in your browser to access the Airflow Webserver and log in with
    * Username = `airflow`
    * Password = `airflow`
12. Go to `Admin > Connections` > Create a new record` in the Airflow Webserver, set
    * `Connection Id` = `google_cloud_default`
    * `Connection Type` = `Google Cloud`
    * `Keyfile Path` = `/opt/airflow/google/credentials.json`
13. Go back to the `DAGs` tab and trigger the DAGs
14. Done 🎉 you should see the prod and stag tables with the transformed weather alerts data in BigQuery
    
