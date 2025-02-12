import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

def read_registration_files(base_folder):
    data = []
    
    # Iterate through each year folder in the base folder
    for year in os.listdir(base_folder):
        year_path = os.path.join(base_folder, year)
        
        if os.path.isdir(year_path):
            # Iterate through each file in the registration sub-folder
            for file_name in os.listdir(year_path):
                if file_name.endswith('.parquet'):  # Changed to look for parquet files
                    # types are general, municipal, and primary
                    election_type = file_name.split('_')[0].lower()
                    if election_type == 'general':
                        election_type = 'General'
                    elif election_type == 'municipal':
                        election_type = 'Municipal'
                    elif election_type == 'primary':
                        election_type = 'Primary'
                    else:
                        election_type = 'Unknown'
                        
                    file_path = os.path.join(year_path, file_name)
                    
                    try:
                        # Read the parquet file and get the row count
                        df = pd.read_parquet(file_path)
                        row_count = df.shape[0]
                        column_count = df.shape[1]
                        
                        print(f"Successfully read {file_path}")
                        print(f"Dimensions: {row_count} rows x {column_count} columns")
                        
                        # Append the data to the list
                        data.append({
                            'Year': year,
                            'Column Count': column_count,
                            'Row Count': row_count,
                            'Election Type': election_type,
                            'Filename': file_name
                        })
                    except Exception as e:
                        print(f"Error reading {file_path}: {str(e)}")
    
    # Create a DataFrame from the collected data
    result_df = pd.DataFrame(data)
    if len(data) == 0:
        print(f"No data found in {base_folder}")
        # Print the directory structure to debug
        print("\nDirectory structure:")
        for root, dirs, files in os.walk(base_folder):
            print(f"\nDirectory: {root}")
            print("Files:", files)
    return result_df

# Example usage
print("\nProcessing registration data...")
register = read_registration_files('bulk_data/registration')
print("\nRegistration data summary:")
print(register)
# output the result to a csv file in the data folder
register.to_csv('data/registration_counts.csv', index=False)

# now run the same thing for returns
print("\nProcessing returns data...")
result = read_registration_files('bulk_data/returns')
print("\nReturns data summary:")
print(result)
# output the result to a csv file in the data folder
result.to_csv('data/returns_counts.csv', index=False)
