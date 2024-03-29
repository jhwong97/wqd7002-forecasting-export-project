import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from scripts.imf_functions import imf_extract_data, imf_transformation
from scripts.gc_functions import upload_to_bigquery

load_dotenv()

storage_client = storage.Client()
bucket_name = "wqd7002_project"
bq_client = bigquery.Client()
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,
                                    write_disposition='WRITE_TRUNCATE',
                                    skip_leading_rows=1,
                                    autodetect=True,)

def imf_transformation_ti(ti, client, bucket_name, blob_name, file_format):
    gcs_uri_list_raw = ti.xcom_pull(task_ids = "raw_data_extract")
    gcs_uri_list = imf_transformation(gcs_uri_list=gcs_uri_list_raw,
                                      client=client,
                                      bucket_name=bucket_name,
                                      blob_name=blob_name,
                                      file_format=file_format)
    return gcs_uri_list

def upload_to_bigquery_ti(ti, client, dataset_name, table_name, job_config):
    gcs_uri_list_transformed = ti.xcom_pull(task_ids = "data_transformation")
    upload_to_bigquery(client=client,
                       dataset_name=dataset_name,
                       table_name=table_name,
                       job_config=job_config,
                       gcs_uri_list=gcs_uri_list_transformed)
    return


# Dag configurations part
default_args = {
    'owner': 'albert',
    'email': ['albertwong345@gmail.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'start_date': datetime(2024, 2, 7)
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'   
}

with DAG(
    dag_id='imf_etl',
    default_args=default_args,
    description="ETL pipeline for World Export",
    schedule='@monthly',
    tags=["wqd7002"]
) as dag:
    
    task1 = PythonOperator(
        task_id='raw_data_extract',
        python_callable=imf_extract_data,
        op_kwargs={"countries": ["W00"],
                   "counterparts": ["W00"],
                   "start_year": 2000,
                   "end_year": 2023,
                   "frequency": "M",
                   "client": storage_client,
                   "bucket_name": bucket_name,
                   "blob_name": ["world_export_raw_data.csv"],
                   "file_format": "csv",}
    )
    
    task2 = PythonOperator(
        task_id='data_transformation',
        python_callable=imf_transformation_ti,
        op_kwargs={"client": storage_client,
                   "bucket_name": bucket_name,
                   "blob_name": ["world_export_transformed_data.csv"],
                   "file_format": "csv",}
    )
    
    task3 = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery_ti,
        op_kwargs={"client": bq_client,
                   "dataset_name": "wqd7002_project",
                   "table_name": ["world_export"],
                   "job_config": job_config,}
    )
    
    task1 >> task2 >> task3