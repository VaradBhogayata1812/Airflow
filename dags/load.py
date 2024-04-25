from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DATASET_NAME = 'loadeddata'
STAGING_TABLE_NAME = 'processedtable'
FINAL_TABLE_NAME = 'loadedlogfiles'

with DAG(
    'load',
    default_args=default_args,
    description='Transfer data from staging to final table in BigQuery.',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['load'],
) as dag:

    setup_final_table = BigQueryCreateEmptyTableOperator(
        task_id='setup_final_table',
        dataset_id=DATASET_NAME,
        table_id=FINAL_TABLE_NAME,
        exists_ok=True,
    )

    transfer_data_to_final_table = BigQueryInsertJobOperator(
        task_id='transfer_data_to_final_table',
        configuration={
            'query': {
                'query': f'SELECT * FROM `{DATASET_NAME}.{STAGING_TABLE_NAME}`',
                'destinationTable': {
                    'projectId': 'etl-project-418923',
                    'datasetId': DATASET_NAME,
                    'tableId': FINAL_TABLE_NAME,
                },
                'writeDisposition': 'WRITE_TRUNCATE',
            }
        },
    )

    setup_final_table >> transfer_data_to_final_table





# from airflow import DAG
# from airflow.utils.dates import days_ago
# from datetime import timedelta
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'email': ['varadbhogayata78@gmail.com'],
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
# }

# DATASET_NAME = 'loadeddata'
# STAGING_TABLE_NAME = 'processedtable'
# FINAL_TABLE_NAME = 'loadedlogfiles'

# with DAG(
#     'load',
#     default_args=default_args,
#     description='Transfer data from staging to final table in BigQuery.',
#     schedule_interval=timedelta(days=1),
#     catchup=False,
#     tags=['load'],
# ) as dag:

#     setup_final_table = BigQueryCreateEmptyTableOperator(
#         task_id='setup_final_table',
#         dataset_id=DATASET_NAME,
#         table_id=FINAL_TABLE_NAME,
#         exists_ok=True,
#         dag=dag,
#     )

#     transfer_data_to_final_table = BigQueryInsertJobOperator(
#         task_id='transfer_data_to_final_table',
#         configuration={
#             'query': {
#                 'query': f'''
#                 SELECT *
#                 FROM `{DATASET_NAME}.{STAGING_TABLE_NAME}`
#                 ''',
#                 'destinationTable': {
#                     'projectId': 'etl-project-418923',
#                     'datasetId': DATASET_NAME,
#                     'tableId': FINAL_TABLE_NAME,
#                 },
#                 'sourceFormat': 'CSV',
#                 'writeDisposition': 'WRITE_TRUNCATE',
#                 'fieldDelimiter': ',',
#                 'skipLeadingRows': 1,
#                 'schema': {
#                     'fields': [
#                         {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
#                         {'name': 'time', 'type': 'TIME', 'mode': 'NULLABLE'},
#                         {'name': 's_ip', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'cs_method', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'cs_uri_stem', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'cs_uri_query', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 's_port', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#                         {'name': 'cs_username', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'c_ip', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'cs_user_agent', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'cs_cookie', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'cs_referer', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'sc_status', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#                         {'name': 'sc_substatus', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#                         {'name': 'sc_win32_status', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#                         {'name': 'sc_bytes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#                         {'name': 'cs_bytes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#                         {'name': 'time_taken', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#                         # {'name': 'postal_code', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         # {'name': 'geo_city', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         # {'name': 'geo_state', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         # {'name': 'geo_country', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'is_crawler', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
#                         {'name': 'TotalBytes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#                         {'name': 'OS', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'Browser', 'type': 'STRING', 'mode': 'NULLABLE'},
#                         {'name': 'Extension', 'type': 'STRING', 'mode': 'NULLABLE'}                    ]
#                 },
#             }
#         },
#         dag=dag,
#     )

#     setup_final_table >> transfer_data_to_final_table
