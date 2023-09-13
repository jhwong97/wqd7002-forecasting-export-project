# import export_raw_extract
from export_raw_extract import export_extract
from logging_export import logger

import pandas as pd
from bs4 import BeautifulSoup

url = "https://metsonline.dosm.gov.my/tradev2/product-coderesult"

raw_data = export_extract(url,2000,2000)

def export_prepare():
    result = BeautifulSoup(raw_data.text, 'html.parser') # Parse the HTML
    table = result.find('table', class_='table-bordered') # Look up for the table
    # Extract table rows
    rows = table.find_all('tr')
    
    individual_data = []
    for row in rows:
        cols = row.find_all(['th', 'td'])
        cols = [col.get_text(strip=True) for col in cols]
        if cols:
            individual_data.append(cols)

    # Select a subset of columns from the first row as column names
    df = pd.DataFrame(individual_data)
    for index, row in df.iterrows():
        row_data = list(row)
        if "GRAND TOTAL" in row_data:
            i = index
    df.drop(df.index[i], inplace=True)
    logger.info(df)

export_prepare()