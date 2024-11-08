from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello World!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 1),
}

with DAG('hello_world_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='hello_world_task',
        python_callable=hello_world
    )

    task
