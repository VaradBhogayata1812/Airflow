from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
import os
from google.cloud import storage

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the bucket name and GCS path for the transformed logs
BUCKET_NAME = 'transformedlogfiles'
GCS_PATH = 'transformed_logs/'
LOCAL_TRANSFORMED_PATH = '/opt/airflow/transformed/W3SVC1/'  # Replace with the path where transformed files are stored
DATASET_NAME = 'loadeddata'
TABLE_NAME = 'loadedlogfiles'

def check_create_bucket(bucket_name):
    """Checks if a GCS bucket exists and creates it if not."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket.create(location='europe-north1')
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

# def upload_files_to_gcs(bucket_name, source_files_path):
#     """Uploads files from local filesystem to GCS."""
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     files_to_upload = [f for f in os.listdir(source_files_path) if os.path.isfile(os.path.join(source_files_path, f))]
#     for file_name in files_to_upload:
#         blob = bucket.blob(GCS_PATH + file_name)
#         blob.upload_from_filename(source_files_path + file_name)
#         print(f"Uploaded {file_name} to {GCS_PATH + file_name}.")

def upload_files_to_gcs(bucket_name, source_files_path):
    """Uploads files from local filesystem to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    files_to_upload = os.listdir(source_files_path)
    for file_name in files_to_upload:
        local_file_path = os.path.join(source_files_path, file_name)
        if os.path.isfile(local_file_path):
            try:
                blob = bucket.blob(f"{GCS_PATH}{file_name}")
                blob.upload_from_filename(local_file_path)
                print(f"Uploaded {file_name} to GCS bucket {bucket_name} at {GCS_PATH}")
            except Exception as e:
                print(f"Failed to upload {file_name} to GCS. Error: {str(e)}")
                
with DAG(
    'load_logs_to_bigquery',
    default_args=default_args,
    description='A DAG to load log files to BigQuery',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'load'],
) as dag:
    
    # Task to check and create the GCS bucket if necessary
    check_create_gcs_bucket = PythonOperator(
        task_id='check_create_gcs_bucket',
        python_callable=check_create_bucket,
        op_kwargs={'bucket_name': BUCKET_NAME},
    )

    # Task to upload files to GCS, this assumes you have already created a list of file paths to upload
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_files_to_gcs,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'source_files_path': '/opt/airflow/transformed/W3SVC1/',
        },
    )
    
    # Task to create an empty table in BigQuery if it doesn't exist
    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id='create_bigquery_table',
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        # Define your schema here if you are not auto-detecting it
        # schema_fields=[{'name': 'field_name', 'type': 'STRING', 'mode': 'NULLABLE'}, ...],
        exists_ok=True,  # Set this to False if you want to recreate the table every time
    )

    # Task to load the data from GCS to BigQuery
    load_to_bq_task = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        configuration={
            'load': {
                'sourceUris': [f'gs://{BUCKET_NAME}/{GCS_PATH}*.log'],
                'destinationTable': {
                    'projectId': 'etl-project-418923',
                    'datasetId': DATASET_NAME,
                    'tableId': TABLE_NAME,
                },
                'sourceFormat': 'NEWLINE_DELIMITED_JSON',  # Update if your files are in a different format
                'writeDisposition': 'WRITE_APPEND',  # Or 'WRITE_TRUNCATE' to overwrite the table
                'autodetect': True,  # Or False if you are providing a schema
            },
        },
    )

# Set up task dependencies
check_create_gcs_bucket >> upload_to_gcs_task>> create_table_task >> load_to_bq_task
