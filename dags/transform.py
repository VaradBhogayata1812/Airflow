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
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

BUCKET_NAME = 'transformedlogfiles'
GCS_PATH = 'transformed_logs/'

def ensure_gcs_bucket_exists(bucket_name):
    """Ensure a Google Cloud Storage bucket exists, create if it does not."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket.create(location='europe-north1')
        print(f"Created new bucket: {bucket_name}")
    else:
        print(f"Bucket {bucket_name} already exists.")

def clean_and_upload_to_gcs(bucket_name, local_directory, gcs_directory):
    """Clear GCS directory and upload new files from a local directory."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=gcs_directory))
    for blob in blobs:
        blob.delete()
        print(f"Deleted {blob.name} from bucket {bucket_name}")
    
    files_to_upload = [f for f in os.listdir(local_directory) if f.endswith('.csv')]
    for file_name in files_to_upload:
        blob_path = os.path.join(gcs_directory, file_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(os.path.join(local_directory, file_name))
        print(f"Uploaded {file_name} to GCS path {blob_path}")

def prepare_output_directory(directory_path):
    """Ensure the output directory exists for transformed files."""
    os.makedirs(directory_path, exist_ok=True)

def process_and_transform_logs(input_directory):
    """Read, transform, and save log files from the input directory."""
    output_directory = input_directory.replace('/data/', '/transformed/')
    prepare_output_directory(output_directory)
    for filename in os.listdir(input_directory):
        if filename.endswith('.log'):
            process_log_file(input_directory, filename, output_directory)

def process_log_file(input_directory, filename, output_directory):
    file_path = os.path.join(input_directory, filename)
    try:
        df = pd.read_csv(file_path, sep='\t', header=None, skiprows=4)
        if df.shape[1] == 15:
            df.columns = [
                'date', 'time', 's_ip', 'cs_method', 'cs_uri_stem', 'cs_uri_query',
                's_port', 'cs_username', 'c_ip', 'cs(User-Agent)', 'sc_status',
                'sc_substatus', 'sc_win32_status', 'time_taken', 'varad'
            ]
            
            transformed_file = filename.replace('.log', '.csv')
            transformed_path = os.path.join(output_directory, transformed_file)

            try:
                df.to_csv(transformed_path, sep='\t', index=False)
                print(f"Transformed and saved using tab separator: {transformed_path}")
            except Exception as tab_error:
                print(f"Error with tab separator: {tab_error}, trying with space separator")
                try:
                    df.to_csv(transformed_path, sep=' ', index=False)
                    print(f"Transformed and saved using space separator: {transformed_path}")
                except Exception as space_error:
                    print(f"Error with space separator: {space_error}")
        else:
            print(f"Column mismatch in {filename}, expected 15 columns before adding 'varad'.")
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")


with DAG(
    'log_file_transformation',
    default_args=default_args,
    description='Transform log files and upload them to GCS',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['log processing', 'GCS'],
) as dag:
    
    create_or_check_bucket = PythonOperator(
        task_id='ensure_bucket_exists',
        python_callable=ensure_gcs_bucket_exists,
        op_kwargs={'bucket_name': BUCKET_NAME},
    )

    transform_logs = PythonOperator(
        task_id='transform_logs',
        python_callable=process_and_transform_logs,
        op_kwargs={'input_directory': '/opt/airflow/gcs/data/W3SVC1/'},
    )

    upload_transformed = PythonOperator(
        task_id='upload_transformed_logs',
        python_callable=clean_and_upload_to_gcs,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'local_directory': '/opt/airflow/transformed/W3SVC1/',
            'gcs_directory': GCS_PATH,
        },
    )

    create_or_check_bucket >> transform_logs >> upload_transformed
