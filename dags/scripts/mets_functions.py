import logging
import pandas as pd
import requests
from airflow.exceptions import AirflowFailException
from bs4 import BeautifulSoup
from typing import Optional
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from scripts.gc_functions import upload_to_bucket, upload_to_bigquery

# Define a function for extracting raw data from METS Online
def mets_request(url,
                 payload: Optional[dict] = None,
                 headers: Optional[dict] = None):
    
    # Specify the maximum number of retry
    MAX_RETRIES = 5
    
    # Define the retry strategy
    retry_strategy = Retry(total = MAX_RETRIES,
                           backoff_factor = 2,
                           status_forcelist = [429, 500, 502, 503, 504],
                           allowed_methods = ["GET", "POST"])
    
    # Create an HTTP adapter with the retry strategy and mount it to session
    adapter = HTTPAdapter(max_retries=retry_strategy)
    
    # Create a new session object
    session = requests.Session()
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    logging.info("Extracting raw data in progress ...")
    # Make a request using the created session object
    raw_data = session.post(url,
                            data = payload,
                            headers = headers,
                            verify = False)
    
    if raw_data.status_code == 200:
        logging.info('SUCCESS: Raw Data has been extracted.')

        # Parse the HTML
        raw_data = BeautifulSoup(raw_data.text, 'html.parser')
        return raw_data

    else:
        logging.error(f'Error: Fail to extract the raw data. ErrorCode: {raw_data.status_code}.')
        raise AirflowFailException('Failure of the task due to encountered error.')

# Define a function for basic preprocessing on the extracted raw html text.
def mets_preprocess(raw_data, 
                    dataframe_name):
    
    # Look up for the table
    result = raw_data.find('table', class_='table-bordered')
    
    # Extract table rows
    rows = result.find_all('tr')
    
    individual_data = []
    for row in rows:
        data = row.find_all(['th', 'td'])
        if data:
            data = [item.get_text(strip=True) for item in data]
            if 'GRAND TOTAL' not in data:
                individual_data.append(data)
    
    logging.info('Converting raw data to dataframe......')
    # Select a subset of columns from the first row as column names
    df = pd.DataFrame(individual_data[1:], columns=individual_data[0])
    # To remove the yearly data column due to redundancy 
    data_month_filter = [col for col in df.columns if '-' not in col or not any(char.isdigit() for char in col)]
    df_list = []
    df_monthly = df.loc[:, data_month_filter]
    df_monthly.name = dataframe_name
    df_list.append(df_monthly)
    logging.info(f'SUCCESS: {dataframe_name} has been created')
    
    return df_list

# Define a function for simple data transformation to ensure the dataset is compatiable with Google BigQuery Schema
def mets_transformation(df_list, 
                        new_column_name, 
                        dataframe_name):
    
    transformed_df_list = []
    replacements = {' ': '_',
                    '&': '',
                    ',': '',
                    '.': '',}
    try:
        for item in df_list:
            df = item.transpose().reset_index()
            column_name = list(df.iloc[2,:])
            edited_column_name = []

            # To replace the selected symbols and empty spaces
            for column in column_name:
                for old, new in replacements.items():
                    column = column.replace(old, new)
                edited_column_name.append(column)

            df = df.iloc[3:]
            df.columns = edited_column_name
            df = df.rename(columns={'PRODUCT_DESCRIPTION': 'date'})

            # To convert the 'date' column to datetime format, whereas the others are converted to numeric.
            for column in df.columns:
                if column == 'date':
                    df[column] = pd.to_datetime(df[column])
                else:
                    df[column] = pd.to_numeric(df[column].str.replace(',', ''))

            # Create a new column which sum all the values from other columns in the same row
            df[new_column_name] = df.iloc[:,1:].sum(axis=1)
            df.name = dataframe_name

            transformed_df_list.append(df)
            logging.info(f"SUCCESS: Dataframe - {dataframe_name} has been transformed.")
            return transformed_df_list
    except Exception as e:
        logging.error(f"Error: {e}")
        raise AirflowFailException('Failure of the task due to encountered error.')
    
# Define a function to execute the full ETL process for METS
def mets_extract(url, 
             dataframe_name,
             payload: Optional[dict] = None,
             headers: Optional[dict] = None,):
    try:
        raw_data = mets_extract(url, payload=payload, headers=headers,)
        raw_df_list = mets_preprocess(raw_data, dataframe_name)
        return raw_df_list
    
    except:
        raise AirflowFailException('Failure of the task due to encountered error.')
