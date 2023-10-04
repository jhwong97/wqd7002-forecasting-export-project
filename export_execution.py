from export_raw_extract import export_extract
from export_preparation import export_prepare
from dotenv import load_dotenv
from logging_export import logger
from typing import Optional

import os

def export_etl(
    url,
    payload: Optional[dict] = None,
    headers: Optional[str] = None):
    # To extract raw data
    raw_data = export_extract(url, payload, headers)

    # To prepare the raw data and convert into df
    df = export_prepare(raw_data)
    
    return df