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
# Task 1
# Convert the credentials to .json file for the usage of GOOGLE_APPLICATION_CREDENTIALS
CREDENTIALS = json.loads(os.getenv('CREDENTIALS'))

# Check if there's an existing credentials file
with open('credentials.json', 'w') as cred_file:
    json.dump(CREDENTIALS, cred_file)
           
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='credentials.json'

countries = ["W00"]
counterparts = ["W00"]
start_year = 2000
end_year = 2023
frequency = "M"
storage_client = storage.Client()
bucket_name_t1 = "wqd7002_project"
blob_name_t1 = ["world_export_raw_data.csv"]
file_format_t1 = "csv"

# Task 2
def imf_transformation_ti(ti, client, bucket_name, blob_name, file_format):
    gcs_uri_list_raw = ti.xcom_pull(task_ids = "raw_data_extract")
    gcs_uri_list = imf_transformation(gcs_uri_list=gcs_uri_list_raw,
                                      client=client,
                                      bucket_name=bucket_name,
                                      blob_name=blob_name,
                                      file_format=file_format)
    return gcs_uri_list

blob_name_t2 = ["world_export_transformed_data.csv"]
file_format_t2 = "csv"

# Dag configurations part
default_args = {
    'owner': 'albert',
    'email': ['albertwong345@gmail.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
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
        op_kwargs={"countries": countries,
                   "counterparts": counterparts,
                   "start_year": start_year,
                   "end_year": end_year,
                   "frequency": frequency,
                   "client": storage_client,
                   "bucket_name": bucket_name_t1,
                   "blob_name": blob_name_t1,
                   "file_format": file_format_t1,}
    )
    
    task2 = PythonOperator(
        task_id='data_transformation',
        python_callable=imf_transformation_ti,
        op_kwargs={"client": storage_client,
                   "bucket_name": bucket_name_t1,
                   "blob_name": blob_name_t2,
                   "file_format": file_format_t2,}
    )
    
    task1 >> task2