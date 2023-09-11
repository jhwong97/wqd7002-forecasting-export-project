import wbgapi as wb
import random
import time

max_retries = 2

for retry in range(max_retries):
        retry_delay = random.randint(10,15)
        try:
            data_raw = wb.data.DataFrame(['NY.GDP.MKTP.KN', 'SP.POP.TOTL', 'PA.NUS.FCRF', 'PX.REX.REER'], 'MYS', timeColumns=True, numericTimeKeys=True).transpose().reset_index()
            print("Data from WBG has been extracted successfully.")
            
            col_names = ['YEAR', 'GDP', 'EXCHANGE RATE', 'REER', 'POPULATION']
            
            # Renaming column names
            data_raw.columns = col_names
            break
        
        except Exception as e:
            print(f"Error: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            
print(data_raw)
