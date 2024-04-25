from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryExecuteQueryOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Constants for BigQuery resources
PROJECT_ID = 'etl-project-418923'
DATASET_NAME = 'loadeddata'
STAGING_TABLE_NAME = 'processedtable'
FINAL_TABLE_NAME = 'loadedlogfiles'
LOCATION = 'europe-north1'

# Function to create a dataset
def create_bigquery_dataset():
    client = bigquery.Client()
    dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION
    dataset = client.create_dataset(dataset, exists_ok=True)

# Define the DAG
with DAG(
    'load',
    default_args=default_args,
    description='Transfer data from staging to final table in BigQuery.',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['load'],
) as dag:

    # Task to create the dataset if it doesn't exist using Python code
    create_dataset = PythonOperator(
        task_id='create_dataset',
        python_callable=create_bigquery_dataset
    )

    # Task to ensure the final table is set up
    setup_final_table = BigQueryCreateEmptyTableOperator(
        task_id='setup_final_table',
        dataset_id=DATASET_NAME,
        table_id=FINAL_TABLE_NAME,
        project_id=PROJECT_ID,
        exists_ok=True
    )

    # Task to transfer data from the staging table to the final table
    transfer_data_to_final_table = BigQueryExecuteQueryOperator(
        task_id='transfer_data_to_final_table',
        sql=f'SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.{STAGING_TABLE_NAME}`',
        destination_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.{FINAL_TABLE_NAME}',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False  # Ensuring to use standard SQL
    )

    # Define dependencies
    create_dataset >> setup_final_table >> transfer_data_to_final_table
