import pdfplumber
import pandas as pd

# Path to the input PDF file and output CSV file
pdf_path = "find_elections/Chester_2025Primary_Unofficial List of Candidates_202503192118312752.pdf"   # update this with your PDF file path
csv_path = "find_elections/output_candidates.csv"  # update this with your desired CSV output path

# This is the header text to remove (if it exists)
header_text = "UNOFFICIAL LIST OF CANDIDATES May 20, 2025 Municipal Primary Chester County, Pennsylvania Updated 3/19/2025"


# List to store all extracted table rows
data = []

with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        # Extract table(s) from the page
        table = page.extract_table()
        if table:
            data.extend(table)

# Check if the first row's first cell contains the header text and remove it if so.
if data and data[0] and header_text in data[0][0]:
    data = data[1:]

# Convert the remaining data to a DataFrame and write to CSV.
df = pd.DataFrame(data)
# drop the first row
df = df.iloc[1:]
df.to_csv(csv_path, index=False, header=False)

print(f"Data successfully written to {csv_path}")
