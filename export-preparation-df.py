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