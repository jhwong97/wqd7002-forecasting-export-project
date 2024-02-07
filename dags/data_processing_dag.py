import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.mets_functions import mets_etl
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud import bigquery

load_dotenv()
# Convert the credentials to .json file for the usage of GOOGLE_APPLICATION_CREDENTIALS
CREDENTIALS = json.loads(os.getenv('CREDENTIALS'))

# Check if there's an existing credentials file
with open('credentials.json', 'w') as cred_file:
    json.dump(CREDENTIALS, cred_file)
        
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='credentials.json'

# Task 1 and Task 2
# Retrieve the csrf_token and Cookie values
csrf_token = os.getenv("csrf_token")
Cookie = os.getenv("cookie")

# URL of targeted site
url = "https://metsonline.dosm.gov.my/tradev2/product-coderesult"

# headers of targeted site
headers = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Cookie": Cookie,
}

# payload of targeted site
payload_export = {
    "_csrf": csrf_token,
    "Tradev2[typeofsearch]": "classification",
    "Tradev2[typedigit]": 7,
    "Tradev2[rangecode1]": 0,
    "Tradev2[rangecode2]": 9,
    # 'Tradev2[code_idcode]': ,
    # 'Tradev2[code_idcodedigit9]': ,
    # 'Tradev2[tradeflow]': ,
    "Tradev2[tradeflow][]": "exports",
    # 'Tradev2[timeframe]': ,
    "Tradev2[timeframe]": "month",
    # 'Tradev2[rangeyear]': ,
    # 'Tradev2[rangeyear2]': ,
    # 'Tradev2[rangeyearone]': ,
    # 'Tradev2[rangemonthone]': ,
    "Tradev2[mothdata]": 2000,
    "Tradev2[mothdata2]": 2023,
    # 'Tradev2[classification_serch]': ,
    # 'Tradev2[country2]': ,
    "Tradev2[geogroup]": 1,
    "Tradev2[geogroup]": 29,
    "Tradev2[codeshowby]": "code",
}

dataframe_name = "malaysia_export"
new_column_name = "my_total_export"
storage_client = storage.Client()
bucket_name = 'wqd7002_project'
bq_client = bigquery.Client()
dataset_name = 'wqd7002_project'
table_name = ['malaysia_export']
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,
                                        write_disposition='WRITE_TRUNCATE',
                                        skip_leading_rows=1,
                                        autodetect=True,)

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
    dag_id='data_processing',
    default_args=default_args,
    description="ETL, Data Query, EDA, Modelling",
    schedule='@monthly',
    tags=['international trade']
) as dag:
    
    task1 = PythonOperator(
        task_id='mets_export_etl',
        python_callable=mets_etl,
        op_kwargs={"url": url,
                   "dataframe_name": dataframe_name,
                   "new_column_name": new_column_name,
                   "storage_client": storage_client,
                   "bucket_name": bucket_name,
                   "bq_client": bq_client,
                   "dataset_name": dataset_name,
                   "table_name": table_name,
                   "job_config":job_config,
                   "payload": payload_export,
                   "headers": headers}
    )
    
    task1