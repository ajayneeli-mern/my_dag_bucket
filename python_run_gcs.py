from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import os

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'run_python_from_gcs',
    default_args=default_args,
    schedule_interval='@daily',  # Set to None for manual trigger
)

# Define the function to download the Python file
def download_and_run_script():
    try:
        # Set up GCS client
        client = storage.Client()
        bucket_name = 'newpipeline001'
        bucket = client.bucket(bucket_name)
        blob = bucket.blob('read_from_api.py')

        # Download the Python file
        local_file = '/tmp/read_from_api.py'
        blob.download_to_filename(local_file)

        # Confirm that the file was downloaded
        if os.path.exists(local_file):
            print(f"Script downloaded successfully: {local_file}")
        else:
            print(f"Failed to download script: {local_file}")

        # Run the downloaded Python file
        exec(open(local_file).read())

    except Exception as e:
        print(f"An error occurred while running the script: {e}")

# Define the PythonOperator to execute the download and run function
run_python_task = PythonOperator(
    task_id='download_and_run_python',
    python_callable=download_and_run_script,
    dag=dag,
)

# Set the task in the DAG
run_python_task