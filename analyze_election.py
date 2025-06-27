import pandas as pd
import numpy as np

# Define column names from metadata
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

returns_columns = [
    'election_year', 'election_type', 'county_code', 'precinct_code',
    'candidate_office_rank', 'candidate_district', 'candidate_party_rank',
    'candidate_ballot_position', 'candidate_office_code', 'candidate_party_code',
    'candidate_number', 'candidate_last_name', 'candidate_first_name',
    'candidate_middle_name', 'candidate_suffix', 'vote_total',
    'yes_vote_total', 'no_vote_total', 'us_congressional_district',
    'state_senatorial_district', 'state_house_district', 'municipality_type_code',
    'municipality_name', 'municipality_breakdown_code_1', 'municipality_breakdown_name_1',
    'municipality_breakdown_code_2', 'municipality_breakdown_name_2',
    'bi_county_code', 'mcd_code', 'fips_code', 'vtd_code',
    'ballot_question', 'record_type', 'previous_precinct_code',
    'previous_us_congressional_district', 'previous_state_senatorial_district',
    'previous_state_house_district'
]

# Read the registration data
reg_df = pd.read_parquet('bulk_data/registration/2024/general_vrstat2024_g(9187)_20241223.parquet')
reg_df.columns = registration_columns

# Read the 2023 primary returns data
ret_df = pd.read_parquet('bulk_data/returns/2023/primary_ElectionReturns_2023_Primary_PrecinctReturns.parquet')
ret_df.columns = returns_columns

# Define target municipalities
target_municipalities = [
    'SPRING CITY', 'ATGLEN', 'COATESVILLE', 'WEST GROVE', 
    'MODENA', 'THORNBURY', 'WEST SADSBURY'
]

# Filter for Chester County data and target municipalities
chester_reg = reg_df[
    (reg_df['county_code'] == 15) & 
    (reg_df['municipality_name'].str.upper().isin(target_municipalities))
]
chester_results = ret_df[
    (ret_df['county_code'] == 15) & 
    (ret_df['municipality_name'].str.upper().isin(target_municipalities))
]

# Create precinct-level registration data
precinct_registration = chester_reg[[
    'municipality_name', 'precinct_code', 
    'municipality_breakdown_name_1',
    'registered_voters_party_1', 'registered_voters_party_2',
    'registered_voters_party_3', 'registered_voters_party_4',
    'registered_voters_party_5'
]].assign(
    total_registered=lambda x: x['registered_voters_party_1'] + 
                              x['registered_voters_party_2'] + 
                              x['registered_voters_party_3'] + 
                              x['registered_voters_party_4'] + 
                              x['registered_voters_party_5'],
    dem_pct=lambda x: (x['registered_voters_party_1'] / x['total_registered'] * 100).round(2),
    rep_pct=lambda x: (x['registered_voters_party_2'] / x['total_registered'] * 100).round(2),
    lib_pct=lambda x: (x['registered_voters_party_4'] / x['total_registered'] * 100).round(2),
    other_pct=lambda x: (x['registered_voters_party_5'] / x['total_registered'] * 100).round(2)
)

# Create precinct-level election data
precinct_election = chester_results.groupby([
    'municipality_name', 'precinct_code', 'candidate_office_code',
    'municipality_breakdown_name_1'
]).agg({
    'vote_total': 'sum',
    'candidate_party_rank': 'first',
    'candidate_last_name': 'first',
    'candidate_first_name': 'first'
}).reset_index()

# Rename municipality_breakdown_name_1 to ward for clarity
precinct_registration = precinct_registration.rename(columns={'municipality_breakdown_name_1': 'ward'})
precinct_election = precinct_election.rename(columns={'municipality_breakdown_name_1': 'ward'})

# Save to CSV files
precinct_registration.to_csv('chester_county_targets_registration.csv', index=False)
precinct_election.to_csv('chester_county_targets_election.csv', index=False)

print("\nPrecinct-level data has been saved to:")
print("- chester_county_targets_registration.csv")
print("- chester_county_targets_election.csv")

# Group registration data by municipality
municipality_analysis = chester_reg.groupby(['municipality_name', 'municipality_type_code']).agg({
    'registered_voters_party_1': 'sum',  # Democratic
    'registered_voters_party_2': 'sum',  # Republican
    'registered_voters_party_3': 'sum',  # Green
    'registered_voters_party_4': 'sum',  # Libertarian
    'registered_voters_party_5': 'sum',  # Other
    'precinct_code': 'count'  # Count of precincts
}).reset_index()

# Calculate total registered voters and derived metrics
municipality_analysis['total_registered'] = (
    municipality_analysis['registered_voters_party_1'] +
    municipality_analysis['registered_voters_party_2'] +
    municipality_analysis['registered_voters_party_3'] +
    municipality_analysis['registered_voters_party_4'] +
    municipality_analysis['registered_voters_party_5']
)

municipality_analysis = municipality_analysis.assign(
    dem_reg=municipality_analysis['registered_voters_party_1'],
    rep_reg=municipality_analysis['registered_voters_party_2'],
    green_reg=municipality_analysis['registered_voters_party_3'],
    lib_reg=municipality_analysis['registered_voters_party_4'],
    other_reg=municipality_analysis['registered_voters_party_5'],
    lib_and_other_pct=lambda x: ((x['registered_voters_party_4'] + x['registered_voters_party_5']) / 
                                x['total_registered'] * 100).round(2),
    party_competition=lambda x: (100 - abs((x['registered_voters_party_1'] / x['total_registered'] * 100) -
                                         (x['registered_voters_party_2'] / x['total_registered'] * 100))).round(2),
    major_party_dominance=lambda x: ((x['registered_voters_party_1'] + x['registered_voters_party_2']) / 
                                    x['total_registered'] * 100).round(2)
)

# Print overall statistics for target municipalities
print("\n=== Target Municipalities Voter Registration Analysis ===")
print(f"\nTotal Municipalities: {len(municipality_analysis)}")
print(f"Total Precincts: {municipality_analysis['precinct_code'].sum()}")
print(f"Total Registered Voters: {municipality_analysis['total_registered'].sum():,}")

print("\nRegistration by Party:")
total_reg = municipality_analysis['total_registered'].sum()
print(f"Democratic: {municipality_analysis['dem_reg'].sum():,} ({municipality_analysis['dem_reg'].sum()/total_reg*100:.1f}%)")
print(f"Republican: {municipality_analysis['rep_reg'].sum():,} ({municipality_analysis['rep_reg'].sum()/total_reg*100:.1f}%)")
print(f"Libertarian: {municipality_analysis['lib_reg'].sum():,} ({municipality_analysis['lib_reg'].sum()/total_reg*100:.1f}%)")
print(f"Green: {municipality_analysis['green_reg'].sum():,} ({municipality_analysis['green_reg'].sum()/total_reg*100:.1f}%)")
print(f"Other: {municipality_analysis['other_reg'].sum():,} ({municipality_analysis['other_reg'].sum()/total_reg*100:.1f}%)")

# Display detailed analysis for each municipality
print("\n=== Detailed Municipality Analysis ===")
print("\nMetrics for each municipality:")
print("- lib_and_other_pct: Combined percentage of Libertarian and Other registrations")
print("- party_competition: How close Dem/Rep registration is (100 = even split)")
print("- major_party_dominance: Combined Dem/Rep percentage (lower is better)")

# Sort by different metrics
print("\nMunicipalities Sorted by Libertarian + Other Registration:")
print(municipality_analysis[[
    'municipality_name', 'municipality_type_code', 'lib_and_other_pct', 'party_competition',
    'lib_reg', 'other_reg', 'total_registered', 'precinct_code'
]].sort_values('lib_and_other_pct', ascending=False).to_string())

print("\nMunicipalities Sorted by Party Competition:")
print(municipality_analysis[[
    'municipality_name', 'municipality_type_code', 'party_competition', 'major_party_dominance',
    'lib_reg', 'other_reg', 'total_registered', 'precinct_code'
]].sort_values('party_competition', ascending=False).to_string())

# Municipality Type Analysis
print("\n=== Analysis by Municipality Type ===")
muni_type_analysis = municipality_analysis.groupby('municipality_type_code').agg({
    'total_registered': 'sum',
    'dem_reg': 'sum',
    'rep_reg': 'sum',
    'lib_reg': 'sum',
    'other_reg': 'sum',
    'municipality_name': 'count'  # Count of municipalities
}).assign(
    lib_and_other_pct=lambda x: ((x['lib_reg'] + x['other_reg']) / x['total_registered'] * 100).round(2),
    party_competition=lambda x: (100 - abs((x['dem_reg'] / x['total_registered'] * 100) -
                                         (x['rep_reg'] / x['total_registered'] * 100))).round(2),
    major_party_dominance=lambda x: ((x['dem_reg'] + x['rep_reg']) / x['total_registered'] * 100).round(2)
)

print("\nStatistics by Municipality Type:")
print(muni_type_analysis[[
    'municipality_name', 'total_registered', 'lib_and_other_pct',
    'party_competition', 'major_party_dominance'
]].to_string())

# Targeting Strategy Summary
print("\n=== 2025 Targeting Strategy Recommendations for Target Municipalities ===")
print("\n1. Primary Targets - High Libertarian/Other Municipalities:")
print("   - Focus on municipalities with highest lib_and_other_pct")
print("   - These areas already show openness to alternative parties")
print("   - Concentrate on converting 'Other' registrants to Libertarian")

print("\n2. Secondary Targets - Competitive Municipalities:")
print("   - Target municipalities with high party_competition and lower major_party_dominance")
print("   - These areas show less entrenchment in two-party politics")
print("   - Message on issues where Libertarians differ from both major parties")

print("\n3. Municipality Type Strategy:")
print("   - Analyze performance by municipality type (borough, township, city)")
print("   - Develop targeted messaging strategies for each type")
print("   - Focus resources on municipality types showing highest potential") 