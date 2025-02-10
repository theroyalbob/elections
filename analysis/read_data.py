import requests
import pandas as pd
import os
from io import BytesIO, StringIO
import shutil

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
    """Read file content based on file extension"""
    try:
        if filename.endswith('.xlsx'):
            return pd.read_excel(BytesIO(response.content))
        elif filename.endswith('.csv'):
            # Try different delimiters
            for delimiter in [',', '\t', '|', ';']:
                try:
                    return pd.read_csv(StringIO(response.text), delimiter=delimiter)
                except:
                    continue
            raise ValueError(f"Could not determine delimiter for {filename}")
        elif filename.endswith('.txt'):
            # Try different delimiters for txt files
            for delimiter in [',', '\t', '|', ';']:
                try:
                    return pd.read_csv(StringIO(response.text), delimiter=delimiter)
                except:
                    continue
            # If all delimiter attempts fail, return raw lines
            return pd.DataFrame(response.text.splitlines())
        else:
            raise ValueError(f"Unsupported file format: {filename}")
    except Exception as e:
        print(f"Error reading {filename}: {str(e)}")
        return None

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

def save_data(data, base_dir, year, election_type, filename):
    """Save data to appropriate directory as parquet"""
    if data is None:
        return
    
    directory = f"{base_dir}/{year}"
    os.makedirs(directory, exist_ok=True)
    
    # Create parquet filename by replacing the original extension
    parquet_filename = os.path.splitext(filename)[0] + '.parquet'
    output_path = f"{directory}/{election_type}_{parquet_filename}"
    
    if not os.path.exists(output_path):
        try:
            data.to_parquet(output_path, index=False)
            print(f"Saved {output_path}")
        except Exception as e:
            print(f"Error saving {output_path}: {str(e)}")

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

def main():
    create_directory_structure()
    
    print("Processing registration data...")
    process_links(linklist_registration, "bulk_data/registration")
    
    print("\nProcessing returns data...")
    process_links(linklist_returns, "bulk_data/returns")

if __name__ == "__main__":
    main()






