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

def transform_log_file(file_path):
    # Define columns based on IIS log format or use dynamic parsing if the format varies
    columns = [
        'date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query',
        's-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'cs(Referer)',
        'sc-status', 'sc-substatus', 'sc-win32-status', 'time-taken'
    ]
    df = pd.read_csv(file_path, delim_whitespace=True, names=columns)
    
    # Transformation: IP Geolocation Lookup
    def get_geolocation(ip):
        try:
            response = requests.get(f'http://ip-api.com/json/{ip}')
            json_response = response.json()
            return json_response['country'], json_response['regionName'], json_response['city']
        except Exception as e:
            return 'Unknown', 'Unknown', 'Unknown'
    
    df['country'], df['region'], df['city'] = zip(*df['c-ip'].apply(get_geolocation))

    # Additional transformations can be added here

    # Save the transformed data
    transformed_path = file_path.replace('/opt/airflow/gcs/data/', '/opt/airflow/transformed/')
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
