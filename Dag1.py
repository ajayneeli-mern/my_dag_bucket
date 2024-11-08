import zipfile
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
from datetime import datetime,timedelta

# Define DAG arguments
default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Constants
BUCKET_NAME = 'data143278'  # GCS bucket name
ZIP_FILE_PATH = 'archive.zip'  # Path to the zip file in GCS
UNZIP_FOLDER = '/tmp/unzipped_files'  # Temporary local folder to store unzipped CSVs
DESTINATION_TABLE_PREFIX = 'just-camera-432415-h9.supermarket'  # BigQuery dataset

# Ensure the temporary folder exists
os.makedirs(UNZIP_FOLDER, exist_ok=True)

# Define a function to download and unzip the file from GCS
def download_and_unzip_gcs(**kwargs):
    gcs_hook = GCSHook()

    # Download the zip file from GCS to a local path
    zip_file_local_path = f"{UNZIP_FOLDER}/archive.zip"
    gcs_hook.download(bucket_name=BUCKET_NAME, object_name=ZIP_FILE_PATH, filename=zip_file_local_path)

    # Unzip the file to the local folder
    with zipfile.ZipFile(zip_file_local_path, 'r') as zip_ref:
        zip_ref.extractall(UNZIP_FOLDER)

# Define a function to load each unzipped CSV file to BigQuery
def load_csv_to_bigquery(**kwargs):
    logical_date = kwargs['logical_date']  # Ensure logical_date is in context

    for file_name in os.listdir(UNZIP_FOLDER):
        if file_name.endswith('.csv'):
            local_file_path = f"{UNZIP_FOLDER}/{file_name}"
            table_name = f"{DESTINATION_TABLE_PREFIX}.{os.path.splitext(file_name)[0]}"  # Use file name as table name

            # Define the BigQuery load task dynamically for each CSV file
            load_task = GoogleCloudStorageToBigQueryOperator(
                task_id=f'load_{os.path.splitext(file_name)[0]}_to_bigquery',
                bucket=BUCKET_NAME,
                source_objects=[f"tmp/unzipped_files/{file_name}"],
                destination_project_dataset_table=table_name,
                source_format='CSV',
                skip_leading_rows=1,  # Skip header if CSV has a header row
                write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it exists
                autodetect=True,  # Enable schema auto-detection
                google_cloud_storage_conn_id='google_cloud_default',  # Use the default GCS connection
                task_concurrency=1,  # Ensure tasks are executed one at a time for each file
                # Pass the execution context to the task
                retries=3,
                retry_delay=timedelta(seconds=10),
                **kwargs
            )
            load_task.execute(context=kwargs)

# Define the DAG
with DAG(
    'gcs_unzip_and_load_to_bigquery_dag',
    default_args=default_args,
    description='Unzip CSV files from GCS and load them to BigQuery',
    schedule_interval=None,  # Set as required (e.g., '0 6 * * *' for daily runs at 6 AM)
    catchup=False,
) as dag:

    # Task to download and unzip the GCS zip file
    unzip_task = PythonOperator(
        task_id='download_and_unzip_gcs',
        python_callable=download_and_unzip_gcs,
        provide_context=True  # Ensure the context is provided
    )

    # Task to load each CSV to BigQuery
    load_task = PythonOperator(
        task_id='load_csv_to_bigquery',
        python_callable=load_csv_to_bigquery,
        provide_context=True  # Ensure the context is provided
    )

    # Set task dependencies
    unzip_task >> load_task  # unzip first, then load to BigQuery
