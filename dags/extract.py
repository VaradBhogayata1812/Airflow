from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from datetime import timedelta
import os

# Define the default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ... [your imports and default_args]

def create_dir_if_not_exists(destination_directory):
    os.makedirs(destination_directory, exist_ok=True)

def download_files_from_gcs(bucket_name, source_blob_prefix, destination_directory):
    """Downloads all files from a GCS bucket with the specified prefix to a local directory."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    os.makedirs(destination_directory, exist_ok=True)
    
    blobs = client.list_blobs(bucket, prefix=source_blob_prefix)
    for blob in blobs:
        # The file_path is the local path where the file will be downloaded
        file_path = os.path.join(destination_directory, os.path.basename(blob.name))
        blob.download_to_filename(file_path)
        print(f"Downloaded {blob.name} to {file_path}")

# Ensure this is at the top level, not indented
with DAG(
    'extract',
    default_args=default_args,
    description='A DAG for extracting log files from GCS',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['extract'],
) as dag:

# Task to create the directory
    create_dir = PythonOperator(
        task_id='create_dir',
        python_callable=create_dir_if_not_exists,
        op_kwargs={'destination_directory': '/opt/airflow/gcs/data/W3SVC1/'},
    )

    # Task to download log files from GCS to the local filesystem
    download_logs = PythonOperator(
        task_id='download_logs',
        python_callable=download_files_from_gcs,
        op_kwargs={
            'bucket_name': 'rawlogfiles',  # Your GCS bucket name
            'source_blob_prefix': 'W3SVC1/',  # Prefix of files to download
            'destination_directory': '/opt/airflow/gcs/data/W3SVC1/',  # Local directory to store files
        },
    )
    create_dir >> download_logs
