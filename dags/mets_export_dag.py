import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from scripts.mets_functions import mets_extract_html, mets_preprocess, mets_transformation
from scripts.gc_functions import upload_to_bigquery, upload_to_bucket

# Task1
load_dotenv()
# Convert the credentials to .json file for the usage of GOOGLE_APPLICATION_CREDENTIALS
CREDENTIALS = json.loads(os.getenv('CREDENTIALS'))

# Check if there's an existing credentials file
with open('credentials.json', 'w') as cred_file:
    json.dump(CREDENTIALS, cred_file)
        
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='credentials.json'

# Task 1
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

storage_client = storage.Client()
bq_client = bigquery.Client()
bucket_name_t1 = "wqd7002_project"
blob_name_t1 = ["my_export_request.html"]
file_format_t1 = "html"

# Task2

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
    dag_id='mets_export_etl',
    default_args=default_args,
    description="ETL pipeline for Malaysia's Total Export Value",
    schedule='@monthly',
    tags=["Malaysia's Export"]
) as dag:
    
    task1 = PythonOperator(
        task_id='raw_html_extract',
        python_callable=mets_extract_html,
        op_kwargs={"url": url,
                   "client": storage_client,
                   "bucket_name": bucket_name_t1,
                   "blob_name": blob_name_t1,
                   "file_format": file_format_t1,
                   "payload": payload_export,
                   "headers": headers}
    )
    
    task1
