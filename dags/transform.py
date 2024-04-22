# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import timedelta
# import os
# import pandas as pd
# from google.cloud import storage

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'email': ['varadbhogayata78@gmail.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# BUCKET_NAME = 'transformedlogfiles'
# GCS_PATH = 'transformed_logs/'

# def check_create_bucket(bucket_name):
#     """Checks if a GCS bucket exists and creates it if not."""
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     if not bucket.exists():
#         bucket.create(location='europe-north1')
#         print(f"Bucket {bucket_name} created.")
#     else:
#         print(f"Bucket {bucket_name} already exists.")

# def replace_files_in_gcs(bucket_name, source_files_path, destination_blob_path):
#     """Deletes existing files in GCS and uploads new ones with detailed logging and error handling."""
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     try:
#         blobs = list(client.list_blobs(bucket, prefix=destination_blob_path))
#         for blob in blobs:
#             blob.delete()
#             print(f"Deleted {blob.name} from GCS bucket {bucket_name}")
#     except Exception as e:
#         print(f"Error deleting files in GCS: {e}")

#     try:
#         files_to_upload = [f for f in os.listdir(source_files_path) if f.endswith('.csv')]
#         print(f"Files to upload: {files_to_upload}")
#         if not files_to_upload:
#             print("No files to upload.")
#         for file_name in files_to_upload:
#             blob = bucket.blob(os.path.join(destination_blob_path, file_name))
#             blob.upload_from_filename(os.path.join(source_files_path, file_name))
#             print(f"Uploaded {file_name} to GCS at {destination_blob_path}")
#     except Exception as e:
#         print(f"Error uploading files to GCS: {e}")

# def create_directory_if_not_exists(directory_path):
#     if not os.path.exists(directory_path):
#         os.makedirs(directory_path, exist_ok=True)

# def transform_log_file(file_path):
#     columns = [
#         'date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query',
#         's-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'cs(Referer)',
#         'sc-status', 'sc-substatus', 'sc-win32-status', 'time-taken'
#     ]
#     df = pd.read_csv(file_path, delim_whitespace=True, names=columns, header=None)
#     df['is_crawler'] = df['cs-uri-stem'].apply(lambda x: 'Yes' if 'robots.txt' in str(x) else 'No')
#     transformed_path = file_path.replace('/opt/airflow/gcs/data/', '/opt/airflow/transformed/').replace('.log', '.csv')
#     create_directory_if_not_exists(os.path.dirname(transformed_path))
#     df.to_csv(transformed_path, index=False)
#     print(f"Transformed data saved to {transformed_path}")

# def transform_files_in_directory(directory_path):
#     for filename in os.listdir(directory_path):
#         if filename.endswith('.log'):
#             transform_log_file(os.path.join(directory_path, filename))

# with DAG(
#     'transform_logs',
#     default_args=default_args,
#     description='A DAG for transforming log files',
#     schedule_interval=timedelta(days=1),
#     catchup=False,
#     tags=['transform'],
# ) as dag:
    
#     transform_task = PythonOperator(
#         task_id='transform_logs',
#         python_callable=transform_files_in_directory,
#         op_kwargs={'directory_path': '/opt/airflow/gcs/data/W3SVC1/'},
#     )

#     check_create_gcs_bucket = PythonOperator(
#         task_id='check_create_gcs_bucket',
#         python_callable=check_create_bucket,
#         op_kwargs={'bucket_name': BUCKET_NAME},
#     )

#     upload_to_gcs_task = PythonOperator(
#         task_id='upload_to_gcs',
#         python_callable=replace_files_in_gcs,
#         op_kwargs={
#             'bucket_name': BUCKET_NAME,
#             'source_files_path': '/opt/airflow/transformed/W3SVC1/',
#             'destination_blob_path': GCS_PATH,
#         },
#     )

#     check_create_gcs_bucket >> transform_task >> upload_to_gcs_task






from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import pandas as pd
from google.cloud import storage

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BUCKET_NAME = 'transformedlogfiles'
GCS_PATH = 'transformed_logs/'

def check_create_bucket(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket.create(location='europe-north1')
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

def replace_files_in_gcs(bucket_name, source_files_path, destination_blob_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=destination_blob_path))
    for blob in blobs:
        blob.delete()
        print(f"Deleted {blob.name} from GCS bucket {bucket_name}")
    
    files_to_upload = [f for f in os.listdir(source_files_path) if f.endswith('.csv')]
    for file_name in files_to_upload:
        blob = bucket.blob(os.path.join(destination_blob_path, file_name))
        blob.upload_from_filename(os.path.join(source_files_path, file_name))
        print(f"Uploaded {file_name} to GCS at {destination_blob_path}")

def transform_log_file(file_path):
    # New column headers, replacing dashes with underscores
    new_headers = [
        'date', 'time', 's_ip', 'cs_method', 'cs_uri_stem', 'cs_uri_query',
        's_port', 'cs_username', 'c_ip', 'cs_User_Agent', 'sc_status',
        'sc_substatus', 'sc_win32_status', 'time_taken'
    ]
    df = pd.read_csv(file_path, delim_whitespace=True, header=None)
    df = df.iloc[:, :-1]
    df.loc[-1] = new_headers
    df.index = df.index + 1 
    df.sort_index(inplace=True)
    transformed_path = file_path.replace('.log', '.csv')
    df.to_csv(transformed_path, index=False, header=False)
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
    
    check_create_gcs_bucket = PythonOperator(
        task_id='check_create_gcs_bucket',
        python_callable=check_create_bucket,
        op_kwargs={'bucket_name': BUCKET_NAME},
    )

    transform_task = PythonOperator(
        task_id='transform_logs',
        python_callable=transform_files_in_directory,
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
