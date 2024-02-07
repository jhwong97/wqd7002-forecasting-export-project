import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

# Define a function for extracting raw data from METS Online
def mets_extract(url,
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

