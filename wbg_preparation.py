from wbg_extraction import wbg_extract
from logging_export import logger
import pandas as pd
import wbgapi as wb

def wbg_prepare(datacode,wbg_raw_df):
    
    data_info = pd.DataFrame(wb.series.list(datacode))
    col_names = data_info['value'].tolist()
    col_names.insert(0, 'Year')
    wbg_raw_df.columns = col_names
    wbg_df = wbg_raw_df
    logger.info(wbg_df)
    
    return wbg_df