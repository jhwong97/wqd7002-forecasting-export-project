import logging
import io
import pandas as pd
import requests
from airflow.exceptions import AirflowFailException
from bs4 import BeautifulSoup
from typing import Optional
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from scripts.gc_functions import read_file_from_gcs

# Define a function for extracting raw data from METS Online
def mets_extract_html(url,
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
    raw_html = session.post(url,
                            data = payload,
                            headers = headers,
                            verify = False)
    data_list = []
    if raw_html.status_code == 200:
        logging.info('SUCCESS: Raw Data has been extracted.')
        result = raw_html.content
        # Parse the HTML
        # result = BeautifulSoup(raw_html.text, 'html.parser')
        data_list.append(result)
        return data_list

    else:
        logging.error(f'Error: Fail to extract the raw data. ErrorCode: {raw_html.status_code}.')
        raise AirflowFailException('Failure of the task due to encountered error.')

# Define a function for basic preprocessing on the extracted raw html text.
def mets_preprocess(gcs_uri_list,
                    client):
    data_list = read_file_from_gcs(gcs_uri_list = gcs_uri_list, client=client)
    df_list = []
    try:
        for item in data_list:
            html_content = BeautifulSoup(item, 'html.parser')
            logging.info("Extracting data from from html contents in progress ...")
            # Look up for the table
            result = html_content.find('table', class_='table-bordered')

            # Extract table rows
            rows = result.find_all('tr')

            individual_data = []
            for row in rows:
                data = row.find_all(['th', 'td'])
                if data:
                    data = [item.get_text(strip=True) for item in data]
                    if 'GRAND TOTAL' not in data:
                        individual_data.append(data)

            logging.info('Converting raw extracted data to dataframe in progress ...')
            # Select a subset of columns from the first row as column names
            df = pd.DataFrame(individual_data[1:], columns=individual_data[0])
            # To remove the yearly data column due to redundancy 
            data_month_filter = [col for col in df.columns if '-' not in col or not any(char.isdigit() for char in col)]
            df_monthly = df.loc[:, data_month_filter]
            df_list.append(df_monthly)
            logging.info(f'SUCCESS: Dataframe has been created')
        return df_list
        
    except Exception as e:
        logging.error(f"Error: {e}")
        raise AirflowFailException('Failure of the task due to encountered error.')

# Define a function for simple data transformation to ensure the dataset is compatiable with Google BigQuery Schema
def mets_transformation(gcs_uri_list,
                        client,
                        new_column_name,):
    
    data_list = read_file_from_gcs(gcs_uri_list = gcs_uri_list, client=client)

    transformed_df_list = []
    replacements = {' ': '_',
                    '&': '',
                    ',': '',
                    '.': '',}
    try:
        for item in data_list:
            item = pd.read_csv(io.BytesIO(item))
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

            transformed_df_list.append(df)
            logging.info(f"SUCCESS: Transformed Dataframe has been created.")
        return transformed_df_list
        
    except Exception as e:
        logging.error(f"Error: {e}")
        raise AirflowFailException('Failure of the task due to encountered error.')
