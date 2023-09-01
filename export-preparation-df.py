# Create an empty list for storing the raw text from data extraction
data_extracted = []

# To extract only the table value
for raw in data_raw:
    individual_data = []
    result = BeautifulSoup(raw.text, 'html.parser') # Parse the HTML
    table = result.find('table', class_='table-bordered') # Look up for the table
    # Extract table rows
    rows = table.find_all('tr')
    for row in rows:
        cols = row.find_all(['th', 'td'])
        cols = [col.get_text(strip=True) for col in cols]
        if not cols:
            next # Ignore improper row structure from adding into the list
        else:
            individual_data.append(cols)
    data_extracted.append(individual_data)
    
start_time = time.time()

# Select a subset of columns from the first row as column names
raw_df = pd.DataFrame()

for item in data_extracted:
    df = pd.DataFrame(item)
    for index, row in df.iterrows():
        row_data = list(row)
        if "GRAND TOTAL" in row_data:
            i = index
    df.drop(df.index[i], inplace=True)
    raw_df = pd.concat([raw_df, df], axis=1, ignore_index=True)
    
raw_df = raw_df.loc[:, ~raw_df.columns.duplicated()]
display(raw_df)

end_time = time.time()
print(f'{end_time-start_time}')