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






from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

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

    # Wait for the table to exist without failing
    wait_for_table = BigQueryTableExistenceSensor(
        task_id='wait_for_table',
        project_id='etl-project-418923',
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        poke_interval=30,  # Check every 30 seconds
    )

    # Delete the table if it exists
    delete_table = BigQueryDeleteTableOperator(
        task_id='delete_table',
        deletion_dataset_table=f'{DATASET_NAME}.{TABLE_NAME}',
        ignore_if_missing=True,  # Do not fail if the table does not exist
        trigger_rule="all_done",  # Ensure this runs regardless of previous task status
    )

    # Create the table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        exists_ok=True,
    )

    # Load data into the table
    load_to_bq = BigQueryInsertJobOperator(
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
                'writeDisposition': 'WRITE_TRUNCATE',
                'autodetect': True,
            },
        },
    )

    wait_for_table >> delete_table >> create_table >> load_to_bq
