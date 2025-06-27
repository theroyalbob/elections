import pandas as pd
import argparse
from pathlib import Path

def get_registration_data(year, county_code):
    """
    Load voter registration data for a specific year and county.
    """
    # Define the base path for registration data
    base_path = Path('bulk_data/registration')
    year_path = base_path / str(year)
    
    # Find the registration file for the specified year
    registration_files = list(year_path.glob('*.parquet'))
    if not registration_files:
        raise FileNotFoundError(f"No registration data found for year {year}")
    
    # Use the first parquet file found (usually there's only one)
    reg_file = registration_files[0]
    
    # Define column names
    registration_columns = [
        'election_year', 'election_type', 'county_code', 'precinct_code',
        'party_1_rank', 'party_1_abbr', 'registered_voters_party_1',
        'party_2_rank', 'party_2_abbr', 'registered_voters_party_2',
        'party_3_rank', 'party_3_abbr', 'registered_voters_party_3',
        'party_4_rank', 'party_4_abbr', 'registered_voters_party_4',
        'party_5_rank', 'party_5_abbr', 'registered_voters_party_5',
        'party_6_rank', 'party_6_abbr', 'registered_voters_party_6',
        'us_congressional_district', 'state_senatorial_district', 'state_house_district',
        'municipality_type_code', 'municipality_name',
        'municipality_breakdown_code_1', 'municipality_breakdown_name_1',
        'municipality_breakdown_code_2', 'municipality_breakdown_name_2',
        'bi_county_code', 'mcd_code', 'fips_code', 'vtd_code',
        'previous_precinct_code', 'previous_us_congressional_district',
        'previous_state_senatorial_district', 'previous_state_house_district'
    ]
    
    # Read the registration data
    reg_df = pd.read_parquet(reg_file)
    reg_df.columns = registration_columns
    
    # Filter by county
    reg_df = reg_df[reg_df['county_code'] == county_code]
    
    return reg_df

def get_county_registration(df):
    """
    Get voter registration counts by party for each municipality in the county.
    """
    # Group by municipality and calculate totals
    municipality_data = df.groupby('municipality_name').agg({
        'registered_voters_party_1': 'sum',
        'registered_voters_party_2': 'sum',
        'registered_voters_party_3': 'sum',
        'registered_voters_party_4': 'sum',
        'registered_voters_party_5': 'sum',
        'registered_voters_party_6': 'sum',
        'party_1_abbr': 'first',
        'party_2_abbr': 'first',
        'party_3_abbr': 'first',
        'party_4_abbr': 'first',
        'party_5_abbr': 'first',
        'party_6_abbr': 'first'
    }).reset_index()
    
    # Calculate total for each municipality
    municipality_data['Total'] = municipality_data[[
        'registered_voters_party_1', 'registered_voters_party_2',
        'registered_voters_party_3', 'registered_voters_party_4',
        'registered_voters_party_5', 'registered_voters_party_6'
    ]].sum(axis=1)
    
    # Rename columns to be more readable
    municipality_data = municipality_data.rename(columns={
        'registered_voters_party_1': 'Party_1_Count',
        'registered_voters_party_2': 'Party_2_Count',
        'registered_voters_party_3': 'Party_3_Count',
        'registered_voters_party_4': 'Party_4_Count',
        'registered_voters_party_5': 'Party_5_Count',
        'registered_voters_party_6': 'Party_6_Count',
        'party_1_abbr': 'Party_1',
        'party_2_abbr': 'Party_2',
        'party_3_abbr': 'Party_3',
        'party_4_abbr': 'Party_4',
        'party_5_abbr': 'Party_5',
        'party_6_abbr': 'Party_6'
    })
    
    # Calculate percentages for each party
    for i in range(1, 7):
        municipality_data[f'Party_{i}_Percentage'] = (
            municipality_data[f'Party_{i}_Count'] / municipality_data['Total'] * 100
        ).round(2)
    
    return municipality_data

def main():
    parser = argparse.ArgumentParser(description='Generate voter registration report by municipality for a county')
    parser.add_argument('year', type=int, help='Election year to analyze')
    parser.add_argument('county_code', type=int, help='County code to analyze')
    
    args = parser.parse_args()
    
    try:
        # Get the registration data
        reg_df = get_registration_data(args.year, args.county_code)
        
        # Get county registration breakdown
        registration = get_county_registration(reg_df)
        
        # Sort by municipality name alphabetically
        registration = registration.sort_values('municipality_name')
        
        # Save to CSV
        output_file = f"{args.county_code}_registration.csv"
        registration.to_csv(output_file, index=False)
        print(f"\nRegistration data saved to {output_file}")
        
        # Print summary
        print(f"\nCounty {args.county_code} Registration Summary ({args.year})")
        print("-" * 60)
        print(f"Total Municipalities: {len(registration)}")
        print(f"Total Registered Voters: {registration['Total'].sum():,}")
        
        # Print first 5 municipalities alphabetically
        print("\nFirst 5 Municipalities Alphabetically:")
        print(registration[['municipality_name', 'Total']].head().to_string(index=False))
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 