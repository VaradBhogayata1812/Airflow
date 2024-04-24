from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import pandas as pd
from google.cloud import storage
import requests
from functools import lru_cache
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

schema_fields = [
    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'time', 'type': 'TIME', 'mode': 'NULLABLE'},
    {'name': 's_ip', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'cs_method', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'cs_uri_stem', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'cs_uri_query', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 's_port', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'cs_username', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'c_ip', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'cs_user_agent', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'cs_cookie', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'cs_referer', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sc_status', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'sc_substatus', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'sc_win32_status', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'sc_bytes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'cs_bytes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'time_taken', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'is_crawler', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}
]

BUCKET_NAME = 'transformedlogfiles'
GCS_PATH = 'transformed_logs/'

def ensure_gcs_bucket_exists(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket.create(location='europe-north1')
        print(f"Created new bucket: {bucket_name} in europe-north1")
    else:
        print(f"Bucket {bucket_name} already exists.")

def clean_and_upload_to_gcs(bucket_name, local_directory, gcs_directory):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=gcs_directory))
    for blob in blobs:
        blob.delete()
        print(f"Deleted {blob.name} from bucket {bucket_name}")
    
    for file_name in os.listdir(local_directory):
        if file_name.endswith('.csv'):
            blob_path = os.path.join(gcs_directory, file_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(os.path.join(local_directory, file_name))
            print(f"Uploaded {file_name} to GCS path {blob_path}")

def prepare_output_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)
        print(f"Created directory {directory_path}")
    else:
        print(f"Directory {directory_path} already exists.")

def is_crawler(df):
    """Identifies crawler IPs based on access to 'robots.txt'."""
    crawler_ips = df[df['cs_uri_stem'] == '/robots.txt']['c_ip'].unique()
    df['is_crawler'] = df['c_ip'].isin(crawler_ips)
    return df

@lru_cache(maxsize=None)
def fetch_geolocation(ip_address):
    token = '7a5b8d9f4da77e'
    url = f"https://ipinfo.io/{ip_address}?token={token}"
    response = requests.get(url)
    data = response.json()
    
    geolocation_data = {
        'postal_code': data.get('postal', ''),
        'city': data.get('city', ''),
        'state': data.get('region', ''),
        'country': data.get('country', '')
    }
    print(f"Geolocation data for IP {ip_address}: {geolocation_data}")
    return geolocation_data

def add_geolocation(df):
    geolocation_series = df['c_ip'].apply(fetch_geolocation)
    df = pd.concat([df, geolocation_series.apply(pd.Series)], axis=1)
    return df

def transform_datetime(df):
    """Transforms date and time columns to BigQuery compatible formats."""
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time
    df['time'] = df['time'].astype(str)
    return df

def process_and_transform_logs(input_directory, output_directory):
    prepare_output_directory(output_directory)
    for filename in os.listdir(input_directory):
        if filename.endswith('.log'):
            process_log_file(input_directory, filename, output_directory)

def process_log_file(input_directory, filename, output_directory):
    file_path = os.path.join(input_directory, filename)
    df = None

    def filter_and_split_lines(file_path, delimiter):
        try:
            with open(file_path, 'r') as file:
                lines = [line for line in file if not line.strip().startswith('#')]
            return pd.DataFrame([line.strip().split(delimiter) for line in lines])
        except Exception as e:
            print(f"Error reading or parsing with {delimiter} delimiter: {e}")
            return None

    df = filter_and_split_lines(file_path, '\t')
    if df is not None and df.shape[1] in [14, 18]:
        print(f"Columns found using tab separator: {df.shape[1]}")
    else:
        df = filter_and_split_lines(file_path, ' ')
        if df is not None:
            print(f"Columns found using space separator: {df.shape[1]}")

    if df is not None:
        base_columns = [
            'date', 'time', 's_ip', 'cs_method', 'cs_uri_stem', 'cs_uri_query',
            's_port', 'cs_username', 'c_ip', 'cs_User_Agent', 'cs_Cookie', 'cs_Referer',
            'sc_status', 'sc_substatus', 'sc_win32_status', 'sc_bytes', 'cs_bytes',
            'time_taken'
        ]
        if df.shape[1] == 14:
            df.columns = [
                'date', 'time', 's_ip', 'cs_method', 'cs_uri_stem', 'cs_uri_query',
                's_port', 'cs_username', 'c_ip', 'cs_User_Agent', 'sc_status',
                'sc_substatus', 'sc_win32_status', 'time_taken'
            ]
            insert_positions = [10, 11, 14, 15]
            missing_columns = ['cs_Cookie', 'cs_Referer', 'sc_bytes', 'cs_bytes']
            for pos, col in zip(insert_positions, missing_columns):
                df.insert(pos, col, None)
        elif df.shape[1] == 18:
            df.columns = base_columns
        else:
            print(f"Column mismatch in {filename}, found {df.shape[1]} columns")
            return

        df = is_crawler(df)
        # df = add_geolocation(df)
        df = transform_datetime(df)
        transformed_file = filename.replace('.log', '.csv')
        transformed_path = os.path.join(output_directory, transformed_file)
        try:
            df.to_csv(transformed_path, sep=',', index=False)
            print(f"Transformed and saved: {transformed_path}")
        except Exception as write_error:
            print(f"Error saving the transformed file: {write_error}")

with DAG(
    'transform',
    default_args=default_args,
    description='Transform log files and upload them to GCS',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['transform'],
) as dag:

    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id='create_bigquery_table',
        dataset_id='loadeddata',
        table_id='stagingtable',
        schema_fields=schema_fields,
        exists_ok=True,
        dag=dag,
    )
    
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=['transformed_logs/*.csv'],
        destination_project_dataset_table='etl-project-418923.loadeddata.stagingtable',
        schema_fields=schema_fields,
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        source_format='CSV',
        dag=dag,
    )

    create_or_check_bucket = PythonOperator(
        task_id='ensure_bucket_exists',
        python_callable=ensure_gcs_bucket_exists,
        op_kwargs={'bucket_name': BUCKET_NAME},
    )

    transform_logs = PythonOperator(
        task_id='transform_logs',
        python_callable=process_and_transform_logs,
        op_kwargs={
            'input_directory': '/opt/airflow/gcs/data/W3SVC1/',
            'output_directory': '/opt/airflow/transformed/W3SVC1/',
        },
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

    

    create_or_check_bucket >> transform_logs >> upload_transformed >> create_bq_table >> load_csv_to_bq
