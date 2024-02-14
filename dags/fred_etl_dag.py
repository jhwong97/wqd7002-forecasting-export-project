import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from scripts.fred_functions import fred_data_extraction, fred_transformation
from scripts.gc_functions import upload_to_bigquery

load_dotenv()

# Convert the credentials to .json file for the usage of GOOGLE_APPLICATION_CREDENTIALS
CREDENTIALS = json.loads(os.getenv('CREDENTIALS'))

# Check if there's an existing credentials file
with open('credentials.json', 'w') as cred_file:
    json.dump(CREDENTIALS, cred_file)
           
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='credentials.json'

fred_api = os.getenv("fred_api")
selected_data = ['EXMAUS', 'RBMYBIS']
storage_client = storage.Client()
bucket_name = "wqd7002_project"

def fred_transformation_ti(ti, selected_data, client, bucket_name, blob_name, file_format):
    gcs_uri_list_raw = ti.xcom_pull(task_ids = "raw_data_extract")
    gcs_uri_list = fred_transformation(gcs_uri_list=gcs_uri_list_raw,
                                       selected_data=selected_data,
                                       client=client,
                                       bucket_name=bucket_name,
                                       blob_name=blob_name,
                                       file_format=file_format)
    return gcs_uri_list
blob_name_t2 = ["my_er_transformed_data.csv", "my_rbeer_transformed_data.csv"]
file_format_t2 = "csv"

def upload_to_bigquery_ti(ti, client, dataset_name, table_name, job_config):
    gcs_uri_list_transformed = ti.xcom_pull(task_ids = "data_transformation")
    upload_to_bigquery(client=client,
                       dataset_name=dataset_name,
                       table_name=table_name,
                       job_config=job_config,
                       gcs_uri_list=gcs_uri_list_transformed)
    return
bq_client = bigquery.Client()
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,
                                    write_disposition='WRITE_TRUNCATE',
                                    skip_leading_rows=1,
                                    autodetect=True,)

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
    dag_id='fred_etl',
    default_args=default_args,
    description="ETL pipeline for Exchange Rate and RBEER",
    schedule='@monthly',
    tags=["exchange rate"]
) as dag:
    
    task1 = PythonOperator(
        task_id='raw_data_extract',
        python_callable=fred_data_extraction,
        op_kwargs={"FRED_API": fred_api,
                   "selected_data": selected_data,
                   "client": storage_client,
                   "bucket_name": bucket_name,
                   "blob_name": ["my_er_raw_data.csv", "my_rbeer_raw_data.csv"],
                   "file_format": "csv",}
    )
    
    task2 = PythonOperator(
        task_id='data_transformation',
        python_callable=fred_transformation_ti,
        op_kwargs={"client": storage_client,
                   "selected_data": selected_data,
                   "bucket_name": bucket_name,
                   "blob_name": ["my_er_transformed_data.csv", "my_rbeer_transformed_data.csv"],
                   "file_format": "csv",}
    )
    
    task3 = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery_ti,
        op_kwargs={"client": bq_client,
                   "dataset_name": "wqd7002_project",
                   "table_name": ["USDMYR", "RBEER"],
                   "job_config": job_config,}
    )
    
    task1 >> task2 >> task3