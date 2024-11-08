from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(dag_id='GCS_to_BQ_and_AGGIGATION',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

# Dummy strat task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

# GCS to BigQuery data load Operator and task
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
                task_id='gcs_to_bq_load',
                bucket='001project',
                source_objects=['movies.csv'],
                destination_project_dataset_table='just-camera-432415-h9.gcp_dataeng_demos.gcs_to_bq_table',
                schema_fields=[

                    {"name": "rank", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "description", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "genre", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "rating", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "year", "type": "INTEGER", "mode": "NULLABLE"}
                ],
                skip_leading_rows=1,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE', 
    dag=dag)

# BigQuery task, operator
    create_aggr_bq_table = BigQueryOperator(
    task_id='create_aggr_bq_table',
    use_legacy_sql=False,
    allow_large_results=True,
    sql="CREATE OR REPLACE TABLE gcp_dataeng_demos.bq_table_aggr AS\
        SELECT year, max(rating)AS avg_rating\
        FROM just-camera-432415-h9.gcp_dataeng_demos.gcs_to_bq_table\
        GROUP BY year", 
    dag=dag)

# Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Settting up task  dependency
start >> gcs_to_bq_load >> create_aggr_bq_table >> end