from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import pandas as pd
import requests

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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

def replace_files_in_gcs(bucket_name, source_files_path, destination_blob_path):
    """Deletes existing files in GCS and uploads new ones from the local filesystem."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # List and delete existing files in the destination_blob_path
    blobs = list(client.list_blobs(bucket, prefix=destination_blob_path))
    for blob in blobs:
        blob.delete()
        print(f"Deleted {blob.name} from GCS bucket {bucket_name}")
    
    # Upload new files from the local filesystem to GCS
    files_to_upload = os.listdir(source_files_path)
    for file_name in files_to_upload:
        local_file_path = os.path.join(source_files_path, file_name)
        if os.path.isfile(local_file_path):
            try:
                new_blob = bucket.blob(f"{destination_blob_path}{file_name}")
                new_blob.upload_from_filename(local_file_path)
                print(f"Uploaded {file_name} to GCS bucket {bucket_name} at {destination_blob_path}")
            except Exception as e:
                print(f"Failed to upload {file_name} to GCS. Error: {str(e)}")

def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)

def transform_log_file(file_path):
    columns = [
        'date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query',
        's-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'cs(Referer)',
        'sc-status', 'sc-substatus', 'sc-win32-status', 'time-taken'
    ]
    df = pd.read_csv(file_path, delim_whitespace=True, names=columns)

    # Convert cs-uri-stem to string to ensure it is iterable
    df['cs-uri-stem'] = df['cs-uri-stem'].astype(str)

    # Modify lambda to check for 'robots.txt' in a safe manner
    df['is_crawler'] = df['cs-uri-stem'].apply(lambda x: 'Yes' if 'robots.txt' in x else 'No')

    # Assume subsequent requests from an IP that requested robots.txt are from the same crawler
    crawler_ips = df[df['is_crawler'] == 'Yes']['c-ip'].unique()
    df['is_crawler'] = df['c-ip'].apply(lambda ip: 'Yes' if ip in crawler_ips else 'No')

    # # Transformation: IP Geolocation Lookup
    # def get_geolocation(ip):
    #     try:
    #         response = requests.get(f'http://ip-api.com/json/{ip}')
    #         json_response = response.json()
    #         return json_response['country'], json_response['regionName'], json_response['city']
    #     except Exception as e:
    #         return 'Unknown', 'Unknown', 'Unknown'
    
    # df['country'], df['region'], df['city'] = zip(*df['c-ip'].apply(get_geolocation))

    # Save the transformed data
    transformed_path = file_path.replace('/opt/airflow/gcs/data/', '/opt/airflow/transformed/')
    create_directory_if_not_exists(os.path.dirname(transformed_path))
    df.to_csv(transformed_path, index=False)
    print(f"Transformed data saved to {transformed_path}")

def transform_files_in_directory(directory_path):
    for filename in os.listdir(directory_path):
        if filename.endswith('.log'):
            transform_log_file(os.path.join(directory_path, filename))

with DAG(
    'transform_logs',
    default_args=default_args,
    description='A DAG for transforming log files',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['transform'],
) as dag:
    
    transform_task = PythonOperator(
        task_id='transform_logs',
        python_callable=transform_files_in_directory,
        op_kwargs={'directory_path': '/opt/airflow/gcs/data/W3SVC1/'},
    )

transform_task
