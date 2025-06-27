import pdfplumber
import pandas as pd

# Inoput pdf_file and output target csv file
pdf_path_candidates = "find_elections/Chester_2025Primary_Unofficial List of Candidates_202503192118312752.pdf"   # update this with your PDF file path
csv_path_candidates = "find_elections/output_candidates_chester.csv"  # update this with your desired CSV output path

pdf_path_main = "/Users/andrewstewart/Projects/Andrew Stewart Consulting/elections/find_elections/Chester_2025Primary_Unofficial list of Offices_202503030930393866 (1).pdf"   # update this with your PDF file path
csv_path_main = "find_elections/output_main_chester.csv"  # update this with your desired CSV output path

# Copy and paste the header text from the PDF file.
header_text_candidates = "UNOFFICIAL LIST OF CANDIDATES May 20, 2025 Municipal Primary Chester County, Pennsylvania Updated 3/19/2025"
header_text_main = "CHESTER COUNTY, PENNSYLVANIA UNOFFICIAL LIST OF OFFICES MAY 20, 2025 Municipal Primary"

#input_list
input_list = [
    {
        "pdf_path": pdf_path_candidates,
        "csv_path": csv_path_candidates,
        "header_text": header_text_candidates
    },
    {
        "pdf_path": pdf_path_main,
        "csv_path": csv_path_main,
        "header_text": header_text_main
    }
]

for input in input_list:
    # List to store extracted table rows for this PDF
    data = []
    
    with pdfplumber.open(input["pdf_path"]) as pdf:
        for page in pdf.pages:
            # Extract table(s) from the page
            table = page.extract_table()
            if table:
                data.extend(table)

    # Check if the first row's first cell contains the header text and remove it if so.
    if data and data[0] and input["header_text"] in data[0][0]:
        data = data[1:]

    # Convert the remaining data to a DataFrame and write to CSV.
    df = pd.DataFrame(data)
    # drop the first row
    df = df.iloc[1:]
    df.to_csv(input["csv_path"], index=False, header=False)

    print(f"Data successfully written to {input['csv_path']}")

# Load the CSV files into DataFrames
df_main = pd.read_csv(csv_path_main)
df_candidate = pd.read_csv(csv_path_candidates)

# Define the output path for the filtered data
filtered_csv = "find_elections/filtered_contests_chester.csv"

# Check that the required columns exist in the main DataFrame
required_columns = ['Contest Long Name', 'Party']
for col in required_columns:
    if col not in df_main.columns:
        raise ValueError(f"'{col}' is missing in the main CSV file.")

# Step 1: Filter out rows from df_main where the contest appears in df_candidate
filtered_df = df_main[~df_main['Contest Long Name'].isin(df_candidate['Contest Long Name'])].copy()

# Create a new column 'Contest_Key' by removing the first 3 characters of 'Contest Long Name'
filtered_df['Contest_Key'] = filtered_df['Contest Long Name'].str[3:]

# Step 2: For each contest (based on Contest_Key), only keep it if it has both 'Republican' and 'Democratic' rows.
# Group by 'Contest_Key' and check that both parties are present.
contests_with_both = filtered_df.groupby('Contest_Key')['Party'].apply(
    lambda parties: {'Republican', 'Democratic'}.issubset(set(parties))
)
valid_contest_keys = contests_with_both[contests_with_both].index

# Filter the DataFrame to only include contests with valid contest keys.
final_df = filtered_df[filtered_df['Contest_Key'].isin(valid_contest_keys)].copy()

# Drop the Contest Long Name column and keep Contest_Key
final_df = final_df.drop(columns=['Contest Long Name'])

# Deduplicate rows based on Contest_Key, keeping the first occurrence
final_df = final_df.drop_duplicates(subset=['Contest_Key'])

# Write the final filtered data to a new CSV file.
final_df.to_csv(filtered_csv, index=False)
print(f"Final filtered data saved to {filtered_csv}")