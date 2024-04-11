from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

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

# Define the DAG, its schedule, and set it to be active
with DAG(
    'extract_logs_gcs',
    default_args=default_args,
    description='A DAG for extracting log files from GCS',
    schedule_interval=timedelta(days=1),
    catchup=False,  # If True, backfill over the start_date range; False prevents backfilling
    tags=['extract'],  # Optional: tags help with filtering DAGs in the UI
) as dag:

    # Task to download log files from GCS to the local filesystem
    download_logs = GCSToLocalFilesystemOperator(
        task_id='download_logs',
        bucket='rawlogfiles',
        object_name='W3SVC1/*', # Use wildcard to download all files in the folder
        filename='/Airflow/gcs/data/{{ ds }}/file_{{ ds_nodash }}.log', # Organizing files by execution date
    )
