from export_raw_extract import export_extract
from logging_export import logger
import pandas as pd
from bs4.element import Tag

def export_prepare(raw_data):
    
    result = raw_data.find('table', class_='table-bordered') # Look up for the table
    # Extract table rows
    if not isinstance(result, Tag):
        return
    
    rows = result.find_all('tr')
    
    individual_data = []
    for row in rows:
        data = row.find_all(['th', 'td'])
        if data:
            data = [item.get_text(strip=True) for item in data]
            if 'GRAND TOTAL' not in data:
                individual_data.append(data)

    logger.info('Preparing raw to dataframe...... ')
    df = pd.DataFrame(individual_data)
    logger.info('SUCCESS: Dataframe has been created.')
    
    return df