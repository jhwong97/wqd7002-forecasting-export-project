from export_input import user_input
from export_raw_extract import export_extract
from export_preparation import export_prepare

result = user_input()
start_year = result[0]
end_year = result[1]

raw_data = export_extract(start_year, end_year)
df = export_prepare(raw_data)