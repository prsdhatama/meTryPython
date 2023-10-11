from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

# Your custom Python functions
from my_custom_operators import FetchDataOperator, ParseDataOperator, InsertDataOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10),
    'retries': 1,
}

dag = DAG(
    'my_data_workflow',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,
)

# Define your custom operators
fetch_data = FetchDataOperator(
    task_id='fetch_data_task',
    game='csgo',
    dag=dag,
)

parse_data = ParseDataOperator(
    task_id='parse_data_task',
    dag=dag,
)

insert_data = InsertDataOperator(
    task_id='insert_data_task',
    dag=dag,
)

# Define task dependencies
fetch_data >> parse_data >> insert_data
