from export_raw_extract import export_extract
from export_preparation import export_prepare
from dotenv import load_dotenv
from logging_export import logger
import os

load_dotenv()
# Retrieve the csrf_token and Cookie values
csrf_token = os.getenv('csrf_token')
Cookie = os.getenv('cookie')

# URL of targeted site
URL = 'https://metsonline.dosm.gov.my/tradev2/product-coderesult'

# headers of targeted site
headers = {
"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", 
"Cookie": Cookie}

# payload of targeted site
payload= {
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
'Tradev2[mothdata]': 2001,
'Tradev2[mothdata2]': 2001,
# 'Tradev2[classification_serch]': ,
# 'Tradev2[country2]': ,
'Tradev2[geogroup]': 1,
'Tradev2[geogroup]': 29,
'Tradev2[codeshowby]': 'code'
}

def export_etl(start_year=2001, end_year=2001, url=URL, payload=payload, headers=headers):
    
    # To replace the payload info, if default year is not used
    payload['Tradev2[mothdata]'] = start_year
    payload['Tradev2[mothdata2]'] = end_year
    
    # To extract raw data
    raw_data = export_extract(url, payload, headers, end_year)
    
    # To prepare the raw data and convert into df
    cleandf = export_prepare(raw_data)
    
    # Save the df into .csv file
    cleandf.to_csv('./export-data-my.csv', index=False)
    logger.info("Done")

export_etl()