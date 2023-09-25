from wbg_extraction import wbg_extract
from wbg_preparation import wbg_prepare
from logging_export import logger


datacode = ['NY.GDP.MKTP.KN', 'SP.POP.TOTL', 'PA.NUS.FCRF', 'PX.REX.REER']
country = ['MYS']

def wbg_etl(datacode=datacode, country=country):
    wbg_raw_df = wbg_extract(datacode, country)
    wbg_df = wbg_prepare(datacode, wbg_raw_df)
    wbg_df.to_csv('.\wbg-data-my.csv', index=False)
    logger.info("Done")

wbg_etl()