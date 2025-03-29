FROM apache/airflow:2.10.5-python3.8

# Install wget inside the container; switch to root since USER airflow does not have permission to run the following command
USER root 
RUN apt-get update && \
    apt-get clean

# Copy and install dependencies
USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Set Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME


