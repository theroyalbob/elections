import pandas as pd

# File paths
main_csv = "find_elections/output.csv"              # CSV with the main data
candidate_csv = "find_elections/output_candidates.csv"  # CSV with candidate data
filtered_csv = "find_elections/filtered_output.csv"    # Output CSV for filtered results


# Read both CSV files into DataFrames
df_main = pd.read_csv(main_csv)
df_candidate = pd.read_csv(candidate_csv)

# Check that the required columns exist in both DataFrames
required_column = 'Contest Long Name'
if required_column not in df_main.columns or required_column not in df_candidate.columns:
    raise ValueError(f"'{required_column}' is missing in one of the CSV files.")

if 'Party' not in df_main.columns:
    raise ValueError("The 'Party' column is missing in the main CSV file.")

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

# Optionally, drop the 'Contest_Key' column if it is not needed in the final output.
final_df = final_df.drop(columns=['Contest_Key'])

# Write the final filtered data to a new CSV file.
final_df.to_csv(filtered_csv, index=False)
print(f"Final filtered data saved to {filtered_csv}")