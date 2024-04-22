from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

BUCKET_NAME = 'transformedlogfiles'
GCS_PATH = 'transformed_logs/'
DATASET_NAME = 'loadeddata'
TABLE_NAME = 'loadedlogfiles'

with DAG(
    'load',
    default_args=default_args,
    description='Load transformed log files into Google BigQuery.',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['load'],
) as dag:

    setup_bigquery_table = BigQueryCreateEmptyTableOperator(
        task_id='setup_bq_table',
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        exists_ok=True,
        dag=dag,
    )

    load_csv_to_bigquery = BigQueryInsertJobOperator(
        task_id='load_csv_to_bq',
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
                'schema': {
                    'fields': [
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
                        {'name': 'cs_referer', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'sc_status', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'sc_substatus', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'sc_win32_status', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'time_taken', 'type': 'INTEGER', 'mode': 'NULLABLE'}
                    ]
                },
            },
        },
        dag=dag,
    )

    setup_bigquery_table >> load_csv_to_bigquery
