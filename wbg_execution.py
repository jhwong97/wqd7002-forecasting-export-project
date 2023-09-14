from wbg_extraction import wbg_extract
from wbg_preparation import wbg_prepare

datacode = ['NY.GDP.MKTP.KN', 'SP.POP.TOTL', 'PA.NUS.FCRF', 'PX.REX.REER']
country = ['MYS']

wbg_raw_df = wbg_extract(datacode, country)
wbg_df = wbg_prepare(datacode, wbg_raw_df)