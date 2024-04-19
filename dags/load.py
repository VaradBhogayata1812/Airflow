# from airflow import DAG
# from airflow.utils.dates import days_ago
# from datetime import timedelta
# from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
# from google.cloud import storage

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'email': ['varadbhogayata78@gmail.com'],
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# BUCKET_NAME = 'transformedlogfiles'
# GCS_PATH = 'transformed_logs/'
# LOCAL_TRANSFORMED_PATH = '/opt/airflow/transformed/W3SVC1/'
# DATASET_NAME = 'loadeddata'
# TABLE_NAME = 'loadedlogfiles'

# with DAG(
#     'load_logs_to_bigquery',
#     default_args=default_args,
#     description='A DAG to load log files to BigQuery',
#     schedule_interval=timedelta(days=1),
#     catchup=False,
#     tags=['etl', 'load'],
# ) as dag:
    
#     create_table_task = BigQueryCreateEmptyTableOperator(
#         task_id='create_bigquery_table',
#         dataset_id=DATASET_NAME,
#         table_id=TABLE_NAME,
#         exists_ok=True,
#     )

#     load_to_bq_task = BigQueryInsertJobOperator(
#         task_id='load_to_bigquery',
#         configuration={
#             'load': {
#                 'sourceUris': [f'gs://{BUCKET_NAME}/{GCS_PATH}*.csv'],
#                 'destinationTable': {
#                     'projectId': 'etl-project-418923',
#                     'datasetId': DATASET_NAME,
#                     'tableId': TABLE_NAME,
#                 },
#                 'sourceFormat': 'CSV',
#                 'writeDisposition': 'WRITE_APPEND',
#                 'autodetect': True,
#             },
#         },
#     )

#     create_table_task >> load_to_bq_task






from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BUCKET_NAME = 'transformedlogfiles'
GCS_PATH = 'transformed_logs/'
DATASET_NAME = 'loadeddata'
TABLE_NAME = 'loadedlogfiles'

with DAG(
    'load_logs_to_bigquery',
    default_args=default_args,
    description='A DAG to load log files to BigQuery',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['load'],
) as dag:

    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id='create_bigquery_table',
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        exists_ok=True,
    )

    load_to_bq_task = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        configuration={
            'load': {
                'sourceUris': [f'gs://{BUCKET_NAME}/{GCS_PATH}*.csv'],
                'destinationTable': {
                    'projectId': 'etl-project-418923',
                    'datasetId': DATASET_NAME,
                    'tableId': TABLE_NAME,
                },
                'sourceFormat': 'CSV',
                'writeDisposition': 'WRITE_TRUNCATE',  # Overwrites the table
                'autodetect': True,
            },
        },
    )

    create_table_task >> load_to_bq_task
