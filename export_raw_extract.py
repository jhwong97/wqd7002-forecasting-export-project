from dotenv import load_dotenv
from logging_export import logger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import os

load_dotenv()
token = os.getenv('csrf_token')
cookie = os.getenv('cookie')

# csrf_token and headers for targeted site
csrf_token = token
headers = {
"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", 
"Cookie": cookie} 

# Function to extract data
def export_extract(url, start_year, end_year, max_retries):
    
    payload_month = {
    '_csrf': csrf_token,
    'Tradev2[typeofsearch]': 'classification',
    'Tradev2[typedigit]': 7,
    'Tradev2[rangecode1]': 0,
    'Tradev2[rangecode2]': 9,
    # 'Tradev2[code_idcode]': ,
    # 'Tradev2[code_idcodedigit9]': ,
    # 'Tradev2[tradeflow]': ,
    'Tradev2[tradeflow][]': 'exports',
    # 'Tradev2[timeframe]': ,
    'Tradev2[timeframe]': 'month',
    # 'Tradev2[rangeyear]': ,
    # 'Tradev2[rangeyear2]': ,
    # 'Tradev2[rangeyearone]': ,
    # 'Tradev2[rangemonthone]': ,
    'Tradev2[mothdata]': start_year,
    'Tradev2[mothdata2]': end_year,
    # 'Tradev2[classification_serch]': ,
    # 'Tradev2[country2]': ,
    'Tradev2[geogroup]': 1,
    'Tradev2[geogroup]': 29,
    'Tradev2[codeshowby]': 'code'
    }
    
    # Define the retry strategy
    retry_strategy = Retry(
        total = max_retries,
        backoff_factor = 1.5,
        status_forcelist = [429, 500, 502, 503, 504]
    )
    
    # Create an HTTP adapter with the retry strategy and mount it to session
    adapter = HTTPAdapter(max_retries=retry_strategy)
    
    # Create a new session object
    session = requests.Session()
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Make a request using the session object
    raw_data = session.post(url, data=payload_month, headers=headers)    
    
    if raw_data.status_code == 200:
        logger.info(f"SUCCESS: Data for {end_year} has been extracted")
    else:
        logger.info(f"FAILED: Data for {end_year} not able to be extracted")
    
    return raw_data