import requests
import pandas as pd
import os
from io import BytesIO, StringIO
import shutil

# Registration column names

linklist_registration = [
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/voterregistration_2024_primary_precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/bulk-election-data/2024-general-election/vr/vrstat2024_g(9187)_20241223.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2023_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2022_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2022_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2021_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2021_Municipal_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2020_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2020_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2019_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2019_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2018_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2018_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2017_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2017_Municipal_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2016_Primary_Precinct.xlsx", # this is an excel file
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2016_General_Precinct.xlsx", # this is an excel file
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2015_Primary_Precinct.xlsx", # this is an excel file
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2015_Municipal_Precinct.csv", # this is a csv file
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2014_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2014_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2012_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2012_General_Precinct.xlsx", # this is an excel file
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2011_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2011_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2010_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2010_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2009_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2008_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2008_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2007_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2007_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2006_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2006_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2004_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2004_General_precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2003_Primary_Precinct.txt", # this is tab separated
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2003_Municipal_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2002_Primary_precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2002_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2001_Primary_PrecinctRetuns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2001_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2000_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_2000_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1999_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1999_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1998_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1998_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1997_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1997_Municipal_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1996_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1996_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1995_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1995_Municipal_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1993_Primary_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/VoterRegistration_1993_Municipal_Precinct.txt",
    
]

linklist_returns = [
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/electionreturns_2024_primary_precinctreturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/bulk-data/erstat_2024_g_268768_20250129.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2023_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2022_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2022_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2021_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2021_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2020_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2020_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2019_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2019_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2018_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2018_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2017_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2017_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2016_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2016_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2015_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2015_Municipal_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2014_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2014_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2012_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2012_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2011_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2011_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2010_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2010_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2009_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2008_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2008_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2007_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2007_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2006_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2006_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2004_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2004_General_Precinct.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2003_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2003_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2002_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2002_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionRetuns_2001_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2001_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2000_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_2000_General_PrecinctRetuns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/bulk-data/ElectionReturns_1999_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/bulk-data/ElectionReturns_1999_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1998_Primary_Precinct_Returns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1998_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1997_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1997_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1996_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1996_General_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1995_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1995_Municipal_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1993_Primary_PrecinctReturns.txt",
    "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/ElectionReturns_1993_Municipal_PrecinctReturns.txt",
    
    

]

def create_directory_structure():
    """Create the necessary directory structure and clean existing files"""
    base_dirs = ['bulk_data/registration', 'bulk_data/returns']
    
    # Remove existing directories and their contents
    for dir in base_dirs:
        if os.path.exists(dir):
            shutil.rmtree(dir)
    
    # Create fresh directories
    for dir in base_dirs:
        os.makedirs(dir, exist_ok=True)

def read_file_content(response, filename):
    """Read file content based on file extension and print dimensions"""
    try:
        if filename.endswith('.xlsx'):
            df = pd.read_excel(BytesIO(response.content), header=None)
            print(f"\n{filename} dimensions: {df.shape}")
                
        elif filename.endswith('.txt'):
            # Try to decode with different encodings
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    response_text = response.content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                print(f"Could not decode {filename} with any supported encoding")
                return None
            
            # Print first few lines to debug
            content = StringIO(response_text)
            first_lines = [next(content) for _ in range(5)]
            print(f"\nFirst few lines of {filename}:")
            for line in first_lines:
                print(line.strip())
            
            # Reset content
            content = StringIO(response_text)
            
            # Try different delimiters and keep track of results
            best_df = None
            best_cols = 1  # Start with 1 since that's what we want to improve upon
            
            # Special case for 2003 primary voter registration which is tab-separated
            if '2003' in filename and 'primary' in filename.lower() and 'registration' in filename.lower():
                try:
                    df = pd.read_csv(StringIO(response_text), delimiter='\t', header=None, encoding=encoding)
                    print(f"\n{filename} dimensions with tab delimiter: {df.shape}")
                    return df
                except Exception as e:
                    print(f"Error reading tab-separated file: {str(e)}")
            
            # For all other files, try delimiters with comma first
            delimiters = [',', '\t', '|', ';']  # Ensure comma is included
            for delimiter in delimiters:
                try:
                    content = StringIO(response_text)
                    df = pd.read_csv(content, delimiter=delimiter, header=None, encoding=encoding)
                    print(f"{filename} dimensions with delimiter '{delimiter}': {df.shape}")
                    
                    # Keep track of the parsing that gives us the most columns
                    if df.shape[1] > best_cols:
                        best_df = df
                        best_cols = df.shape[1]
                except:
                    continue
            
            # If no delimiter worked well, try fixed width as last resort
            if best_cols == 1:
                try:
                    content = StringIO(response_text)
                    df = pd.read_fwf(content, header=None, encoding=encoding)
                    print(f"\n{filename} dimensions with fixed width: {df.shape}")
                    if df.shape[1] > best_cols:
                        best_df = df
                except:
                    pass
            
            return best_df if best_df is not None else None
                    
        elif filename.endswith('.csv'):
            # Try to decode with different encodings
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    response_text = response.content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                print(f"Could not decode {filename} with any supported encoding")
                return None
                
            for delimiter in [',', '\t', '|', ';']:
                try:
                    df = pd.read_csv(StringIO(response_text), delimiter=delimiter, header=None, encoding=encoding)
                    print(f"\n{filename} dimensions with delimiter '{delimiter}': {df.shape}")
                    break
                except:
                    continue
            else:
                raise ValueError(f"Could not determine delimiter for {filename}")

        return df

    except Exception as e:
        print(f"Error reading {filename}: {str(e)}")
        return None

def process_codes(df, is_registration):
    """Process code columns using the dictionary mappings"""
    return df  # For now, just return the dataframe as is

def parse_filename(filename):
    """Parse year and election type from filename"""
    parts = filename.split('_')
    try:
        # First try the standard format
        year = next(part for part in parts if part.isdigit())
        election_type = next(part.lower() for part in parts 
                           if part.lower() in ['primary', 'general', 'municipal'])
    except StopIteration:
        # Handle non-standard format like 'vrstat2024_g(9187)_20241223.txt'
        try:
            # Look for year in any part of the filename
            year = next(part for part in filename.replace('(', '_').replace(')', '_').split('_') 
                       if part.isdigit() and len(part) == 4)
            
            # Look for election type indicator
            if '_g' in filename.lower():
                election_type = 'general'
            elif '_p' in filename.lower():
                election_type = 'primary'
            elif '_m' in filename.lower():
                election_type = 'municipal'
            else:
                raise ValueError("Could not determine election type")
                
        except (StopIteration, ValueError):
            print(f"Could not parse year or election type from {filename}")
            return None, None
            
    return year, election_type

def clean_column_name(col_name):
    """Clean column names to ensure they are valid for parquet"""
    if not isinstance(col_name, str):
        return f'col_{col_name}'
    
    # Replace problematic characters
    cleaned = str(col_name).strip()
    cleaned = ''.join(c if c.isalnum() or c in ['_', '-'] else '_' for c in cleaned)
    
    # Ensure it starts with a letter or underscore
    if cleaned and not (cleaned[0].isalpha() or cleaned[0] == '_'):
        cleaned = 'col_' + cleaned
        
    # If empty or None, generate a default name
    if not cleaned:
        cleaned = 'unnamed_column'
        
    return cleaned

def save_data(data, base_dir, year, election_type, filename):
    """Save data to appropriate directory as parquet"""
    if data is None:
        return
    
    try:
        # Ensure we have a proper DataFrame
        if not isinstance(data, pd.DataFrame):
            print(f"Error: Data for {filename} is not a DataFrame")
            return
            
        # Clean column names
        data.columns = [clean_column_name(col) for col in data.columns]
        
        # Convert all object columns to string and handle encoding issues
        for col in data.columns:
            if data[col].dtype == 'object':
                # Convert to string and handle encoding issues
                data[col] = data[col].apply(lambda x: str(x).encode('utf-8', errors='ignore').decode('utf-8') if x is not None else '')
        
        # Create directory structure
        directory = f"{base_dir}/{year}"
        os.makedirs(directory, exist_ok=True)
        
        # Create parquet filename
        parquet_filename = os.path.splitext(filename)[0] + '.parquet'
        output_path = f"{directory}/{election_type}_{parquet_filename}"
        
        if not os.path.exists(output_path):
            try:
                # Save using pyarrow engine explicitly
                data.to_parquet(output_path, engine='pyarrow', index=False)
                
                # Verify the file was written correctly
                try:
                    pd.read_parquet(output_path)
                    print(f"Successfully saved and verified {output_path}")
                except Exception as e:
                    print(f"Warning: File was saved but verification failed for {output_path}: {str(e)}")
                    
            except Exception as e:
                print(f"Error saving {output_path}: {str(e)}")
                
    except Exception as e:
        print(f"Error processing data for {filename}: {str(e)}")
        return

def process_links(linklist, base_dir):
    """Process list of links and save data"""
    for link in linklist:
        try:
            response = requests.get(link)
            if response.status_code != 200:
                print(f"Failed to retrieve {link}")
                continue

            filename = link.split("/")[-1]
            data = read_file_content(response, filename)
            if data is None:
                continue

            year, election_type = parse_filename(filename)
            if year and election_type:
                save_data(data, base_dir, year, election_type, filename)
            
        except Exception as e:
            print(f"Error processing {link}: {str(e)}")

def generate_column_names(num_columns):
    return [f'col_{i+1}' for i in range(num_columns)]

def main():
    create_directory_structure()
    
    print("Processing registration data...")
    process_links(linklist_registration, "bulk_data/registration")
    
    print("\nProcessing returns data...")
    process_links(linklist_returns, "bulk_data/returns")

if __name__ == "__main__":
    main()