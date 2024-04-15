from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
import glob

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BUCKET_NAME = 'your-bucket-name'
GCS_PATH = 'path/to/your/gcs/folder'
LOCAL_TRANSFORMED_PATH = '/opt/airflow/transformed/'  # Replace with the path where transformed files are stored
DATASET_NAME = 'loadeddata'
TABLE_NAME = 'loadedlogfiles'

def upload_files_to_gcs(bucket_name, gcs_path, local_path):
    """
    Uploads all files from the specified local path to GCS
    """
    hook = GCSHook()
    files = glob.glob(f'{local_path}/*.log')
    for file in files:
        filename = file.split('/')[-1]
        gcs_uri = f'{gcs_path}/{filename}'
        hook.upload(bucket_name, gcs_uri, file)
        print(f'Uploaded {file} to {gcs_uri}')

with DAG(
    'load_logs_to_bigquery',
    default_args=default_args,
    description='A DAG to load transformed logs to BigQuery',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['load'],
) as dag:

    upload_to_gcs_task = PythonOperator(
        task_id='upload_logs_to_gcs',
        python_callable=upload_files_to_gcs,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'gcs_path': GCS_PATH,
            'local_path': LOCAL_TRANSFORMED_PATH,
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
                'sourceUris': [f'gs://{BUCKET_NAME}/{GCS_PATH}/*.log'],
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

    # Set the task dependencies
    upload_to_gcs_task >> create_table_task >> load_to_bq_task
