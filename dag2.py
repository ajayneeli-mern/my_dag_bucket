from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import zipfile
import os
from datetime import timedelta

# Define GCP project ID and GCS paths
PROJECT_ID = "just-camera-432415-h9"
BUCKET_NAME = "data143278"
ARCHIVE_PATH = "archive.zip"
DESTINATION_FOLDER = "archive/"

def unzip_and_upload_to_gcs(bucket_name, source_zip, destination_folder):
    # Initialize GCS client with specified project
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(bucket_name)
    
    # Download the zip file from GCS
    blob = bucket.blob(source_zip)
    local_zip_path = "/tmp/archive.zip"
    blob.download_to_filename(local_zip_path)

    # Unzip and upload each file to the destination folder in GCS
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            # Extract the file locally
            zip_ref.extract(file_info, "/tmp")
            local_file_path = f"/tmp/{file_info.filename}"
            
            # Upload each file to the new destination in GCS
            destination_blob = bucket.blob(f"{destination_folder}{file_info.filename}")
            destination_blob.upload_from_filename(local_file_path)
            
            # Clean up local file after upload
            os.remove(local_file_path)

    # Remove the local zip file
    os.remove(local_zip_path)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'unzip_and_move_files_in_gcs',
    default_args=default_args,
    description='Unzip file from one GCS folder and move to another',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['gcs', 'unzip'],
) as dag:

    unzip_task = PythonOperator(
        task_id='unzip_and_upload_files',
        python_callable=unzip_and_upload_to_gcs,
        op_args=[BUCKET_NAME, ARCHIVE_PATH, DESTINATION_FOLDER],
    )

    

    unzip_task
