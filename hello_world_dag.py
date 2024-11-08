from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval='45 18 * * *',  # Run manually or set a schedule
)

# Define the Python function that will be called by the PythonOperator
def print_hello():
    print("Hello World")

# Create a dummy start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Create the PythonOperator to run the print_hello function
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Create a dummy end task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set the task dependencies
start >> hello_task >> end