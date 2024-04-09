from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Define the default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['varadbhogayata78@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG, its schedule, and set it to be active
dag = DAG(
    'hello_airflow',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# Define the task 'print_hello', which is a BashOperator to execute a command
print_hello = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello, Airflow!"',
    dag=dag,
)

# This is where you would usually define more tasks and their order of execution
# For now, we have just one task

# This code will make the 'print_hello' task an independent task in the DAG
print_hello
