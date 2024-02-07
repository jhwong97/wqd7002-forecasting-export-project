import logging
import pandas as pd
from airflow.exceptions import AirflowFailException
from fredapi import Fred
from scripts.gc_functions import upload_to_bucket, upload_to_bigquery

# Function to extract data from fred using API request
def fred_data_extraction(FRED_API, selected_data):
    df_list = []
    fred = Fred(api_key = FRED_API)
    try:
        # Extract the data based on the code stated in selected data
        for item in selected_data:
            logging.info(f"Extracting {item} data in progress ...")
            df = fred.get_series_all_releases(item)
            logging.info(f"SUCCESS: {item} data has been extracted.")
            df.name = item
            df_list.append(df)
        return df_list
    except Exception as e:
        logging.error("Error: {e}")
        raise AirflowFailException('Failure of the task due to encountered error.')

# Function to perform data transformation such as changing the data type and renaming column name
def fred_transformation(df_list, selected_data):
    transformed_df_list = []
    for i in range(len(df_list)):
        df = df_list[i]
        df['realtime_start'] = pd.to_datetime(df['realtime_start'])
        df['date'] = pd.to_datetime(df['date'])
        df = df.rename(columns={'value': selected_data[i]})
        df = df.drop_duplicates(subset='date')
        df.name = selected_data[i]
        print(f"SUCCESS: {selected_data[i]} dataset has been transformed.")
        transformed_df_list.append(df)
    return transformed_df_list

# Define a function to execute the full ETL process for Fred data
def fred_etl(FRED_API,
             selected_data,
             storage_client,
             bucket_name,
             bq_client,
             dataset_name,
             table_name,
             job_config
             ):
    try:
        df_list = fred_data_extraction(FRED_API=FRED_API, selected_data=selected_data)
        transformed_df_list = fred_transformation(df_listd=df_list, selected_data=selected_data)
        gsutil_uri_list = upload_to_bucket(storage_client=storage_client,
                                        bucket_name=bucket_name,
                                        df_list=transformed_df_list)
        upload_to_bigquery(client=bq_client,
                        dataset_name=dataset_name,
                        table_name=table_name,
                        job_config=job_config,
                        gsutil_uri=gsutil_uri_list)
        return None
    
    except:
        raise AirflowFailException('Failure of the task due to encountered error.')
