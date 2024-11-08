from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from datetime import timedelta
import os

# Define your GCP project and GCS bucket and paths
PROJECT_ID = "just-camera-432415-h9"
BUCKET_NAME = "data143278"
GCS_FOLDER_PATH = "new/archive/"  # Folder where unzipped CSVs are stored
DATASET_ID = "supermarket"  # Replace with your BigQuery dataset ID

def list_csv_files(bucket_name, folder_path):
    """List all CSV files in the specified GCS folder."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return [blob.name for blob in blobs if blob.name.endswith('.csv')]

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'upload_csvs_to_bigquery',
    default_args=default_args,
    description='Upload CSV files in GCS to BigQuery with auto-detected schema and table name from file',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['gcs', 'bigquery', 'auto_schema'],
) as dag:

    # Task 1: List CSV files in the specified GCS folder
    list_csv_files_task = PythonOperator(
        task_id='list_csv_files',
        python_callable=list_csv_files,
        op_args=[BUCKET_NAME, GCS_FOLDER_PATH],
    )

    # Task 2: Load each CSV file to BigQuery with auto-detected schema
    def load_csv_to_bigquery(csv_files):
            
        """Create BigQuery load jobs for each CSV file."""
        load_tasks = []
        for csv_file_path in csv_files:
            table_name = os.path.splitext(os.path.basename(csv_file_path))[0]  # Get table name from CSV file name

            # BigQuery load job configuration with schema auto-detection
            load_task = BigQueryInsertJobOperator(
                task_id=f'load_{table_name}_to_bigquery',
                configuration={
                    "load": {
                        "sourceUris": [f"gs://{BUCKET_NAME}/{csv_file_path}"],
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": DATASET_ID,
                            "tableId": table_name,
                        },
                        "sourceFormat": "CSV",
                        "writeDisposition": "WRITE_TRUNCATE",  # Overwrite table if exists
                        "autodetect": True,  # Auto-detect schema
                        "skipLeadingRows": 1  # Skip header row
                    }
                },
                location='US',  # Change to your BigQuery dataset location if needed
            )
            load_tasks.append(load_task)
        return load_tasks


    # Dynamically create load tasks for each CSV file
    csv_files = list_csv_files(BUCKET_NAME, GCS_FOLDER_PATH)
    load_csv_tasks = load_csv_to_bigquery(csv_files)

    # Set task dependencies
    list_csv_files_task >> load_csv_tasks
