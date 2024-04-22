from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import pandas as pd
from google.cloud import storage

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

BUCKET_NAME = 'transformedlogfiles'
GCS_PATH = 'transformed_logs/'

def check_create_bucket(bucket_name):
    """Checks if a GCS bucket exists and creates it if not."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket.create(location='europe-north1')
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

def replace_files_in_gcs(bucket_name, source_files_path, destination_blob_path):
    """Deletes existing files in GCS and uploads new ones."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=destination_blob_path))
    for blob in blobs:
        blob.delete()
        print(f"Deleted {blob.name} from GCS bucket {bucket_name}")
    
    files_to_upload = [f for f in os.listdir(source_files_path) if f.endswith('.csv')]
    print(f"Files to upload: {files_to_upload}")
    for file_name in files_to_upload:
        blob = bucket.blob(os.path.join(destination_blob_path, file_name))
        blob.upload_from_filename(os.path.join(source_files_path, file_name))
        print(f"Uploaded {file_name} to GCS at {destination_blob_path}")

def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)

def transform_log_files(directory_path):
    create_directory_if_not_exists(directory_path.replace('/opt/airflow/gcs/data/', '/opt/airflow/transformed/'))
    for filename in os.listdir(directory_path):
        if filename.endswith('.log'):
            file_path = os.path.join(directory_path, filename)
            try:
                # Read the log file, skipping the first four lines which are headers/metadata
                df = pd.read_csv(file_path, sep='\t', header=None, skiprows=4)
                # Check if dataframe has the expected number of columns
                if df.shape[1] == 14:
                    df.columns = [
                        'date', 'time', 's_ip', 'cs_method', 'cs_uri_stem', 'cs_uri_query',
                        's_port', 'cs_username', 'c_ip', 'cs_User_Agent', 'sc_status',
                        'sc_substatus', 'sc_win32_status', 'time_taken'
                    ]
                    transformed_path = file_path.replace('/opt/airflow/gcs/data/', '/opt/airflow/transformed/').replace('.log', '.csv')
                    df.to_csv(transformed_path, index=False)
                    print(f"Transformed data saved to {transformed_path}")
                else:
                    print(f"Skipped {file_path} due to column mismatch: expected 14, found {df.shape[1]}")
            except Exception as e:
                print(f"Failed to process {file_path}: {str(e)}")

with DAG(
    'transform_logs',
    default_args=default_args,
    description='A DAG for transforming log files',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['transform'],
) as dag:
    
    check_create_gcs_bucket = PythonOperator(
        task_id='check_create_gcs_bucket',
        python_callable=check_create_bucket,
        op_kwargs={'bucket_name': BUCKET_NAME},
    )

    transform_task = PythonOperator(
        task_id='transform_log_files',
        python_callable=transform_log_files,
        op_kwargs={'directory_path': '/opt/airflow/gcs/data/W3SVC1/'},
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=replace_files_in_gcs,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'source_files_path': '/opt/airflow/transformed/W3SVC1/',
            'destination_blob_path': GCS_PATH,
        },
    )

    check_create_gcs_bucket >> transform_task >> upload_to_gcs_task
