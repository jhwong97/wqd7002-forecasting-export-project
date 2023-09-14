import wbgapi as wb
from logging_export import logger

def wbg_extract(datacode, country):
    
    MAX_RETRIES = 2
    RETRY_DELAY = 5

    for retry in range(MAX_RETRIES):
            try:
                wbg_raw_df = wb.data.DataFrame(datacode, country, timeColumns=True, numericTimeKeys=True).transpose().reset_index()                  
                logger.info("Data from WBG has been extracted successfully.")
                break
            
            except Exception as e:
                print(f"Error: {e}. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                
    return wbg_raw_df