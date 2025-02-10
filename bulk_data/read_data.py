import requests
import pandas as pd
import os
from io import BytesIO, StringIO
import shutil

# Registration column names
registration_columns = [
    "election_year",
    "election_type",
    "county_code",
    "precinct_code",
    'party_1_rank',
    'party_1_abbreviation',
    'part_1_registrations',
    'party_2_rank',
    'party_2_abbreviation',
    'part_2_registrations',
    'party_3_rank',
    'party_3_abbreviation',
    'part_3_registrations',
    'party_4_rank',
    'party_4_abbreviation',
    'part_4_registrations',
    'party_5_rank',
    'party_5_abbreviation',
    'part_5_registrations',
    'party_6_rank',
    'party_6_abbreviation',
    'part_6_registrations',
    'congressional_district',
    'state_senate_district',
    'state_house_district',
    'municipality_type_code',
    'municipality_name',
    'municipality_breakdown_code_1',
    'municipality_breakdown_name_1',
    'municipality_breakdown_code_2',
    'municipality_breakdown_name_2',
    'second_county_code',
    'mcd_code',
    'vtd_code',
    'prev_precinct_code',
    'prev_congressional_district',
    'prev_state_senate_district',
    'prev_state_house_district'
]
# returns column names
returns_columns = [
    'election_year',
    'election_type',
    'county_code',
    'candidate_office_rank',
    'candidate_district',
    'candidate_party_rank',
    'candidate_ballot_position',
    'candidate_office_code',
    'candidate_party_code',
    'candidate_number',
    'candidate_last_name',
    'candidate_first_name',
    'candidate_middle_name',
    'candidate_suffix',
    'vote_total',
    'yes_votes',
    'no_votes',
    'congressional_district',
    'state_senate_district',
    'state_house_district',
    'municipality_type_code',
    'municipality_name',
    'municipality_breakdown_code_1',
    'municipality_breakdown_name_1',
    'municipality_breakdown_code_2',
    'municipality_breakdown_name_2',
    'second_county_code',
    'mcd_code',
    'vtd_code',
    'ballot_question',
    'record_type',
    'prev_precinct_code',
    'prev_congressional_district',
    'prev_state_senate_district',
    'prev_state_house_district'
]

# County code dictionary
county_code_dict = {
    '01': 'Adams',
    '02': 'Allegheny',
    '03': 'Armstrong',
    '04': 'Beaver',
    '05': 'Bedford',
    '06': 'Berks',
    '07': 'Blair',
    '08': 'Bradford',
    '09': 'Bucks',
    '10': 'Butler',
    '11': 'Cambria',
    '12': 'Cameron',
    '13': 'Carbon',
    '14': 'Centre',
    '15': 'Chester',
    '16': 'Clarion',
    '17': 'Clearfield',
    '18': 'Clinton',
    '19': 'Columbia',
    '20': 'Crawford',
    '21': 'Cumberland',
    '22': 'Dauphin',
    '23': 'Delaware',
    '24': 'Elk',
    '25': 'Erie',
    '26': 'Fayette',
    '27': 'Forest',
    '28': 'Franklin',
    '29': 'Fulton',
    '30': 'Greene',
    '31': 'Huntingdon',
    '32': 'Indiana',
    '33': 'Jefferson',
    '34': 'Juniata',
    '35': 'Lackawanna',
    '36': 'Lancaster',
    '37': 'Lawrence',
    '38': 'Lebanon',
    '39': 'Lehigh',
    '40': 'Luzerne',
    '41': 'Lycoming',
    '42': 'McKean',
    '43': 'Mercer',
    '44': 'Mifflin',
    '45': 'Monroe',
    '46': 'Montgomery',
    '47': 'Montour',
    '48': 'Northampton',
    '49': 'Northumberland',
    '50': 'Perry',
    '51': 'Philadelphia',
    '52': 'Pike',
    '53': 'Potter',
    '54': 'Schuylkill',
    '55': 'Snyder',
    '56': 'Somerset',
    '57': 'Sullivan',
    '58': 'Susquehanna',
    '59': 'Tioga',
    '60': 'Union',
    '61': 'Venango',
    '62': 'Warren',
    '63': 'Washington',
    '64': 'Wayne',
    '65': 'Westmoreland',
    '66': 'Wyoming',
    '67': 'York'
}
# municipality type code dictionary
municipality_type_code_dict = {
    '2': 'City',
    '6': 'Borough',
    '4': 'Township',
    '5': 'Town'
}
# municipality breakdown code dictionary
municipality_breakdown_code_dict = {
    'W': 'Ward',
    'D': 'District',
    'P': 'Precinct',
    'X': 'Other'
}

#election type dictionary
election_type_dict = {
    'P': 'Primary',
    'G': 'General',
    'M': 'Municipal',
    'S': 'Special'
}

#office code dictionary
office_code_dict = {
    'USP': 'PRESIDENT OF THE UNITED STATES',
    'USS': 'UNITED STATES SENATOR',
    'GOV': 'GOVERNOR',
    'LTG': 'LIEUTENANT GOVERNOR',
    'ATT': 'ATTORNEY GENERAL',
    'AUD': 'AUDITOR GENERAL',
    'TRE': 'STATE TREASURER',
    'USC': 'REPRESENTATIVE IN CONGRESS',
    'STS': 'SENATOR IN THE GENERAL ASSEMBLY',
    'STH': 'REPRESENTATIVE IN THE GENERAL ASSEMBLY',
    'SPM': 'JUSTICE OF THE SUPREME COURT',
    'SPR': 'JUDGE OF THE SUPERIOR COURT',
    'CCJ': 'JUDGE OF THE COMMONWEALTH COURT',
    'CPJP': 'JUDGE OF THE COURT OF COMMON PLEAS - PHILADELPHIA',
    'CPJA': 'JUDGE OF THE COURT OF COMMON PLEAS - ALLEGHENY',
    'CPJ': 'JUDGE OF THE COURT OF COMMON PLEAS',
    'MCJ': 'JUDGE OF THE MUNICIPAL COURT',
    'DED': 'DELEGATE TO DEMOCRATIC NATIONAL CONVENTION',
    'DER': 'DELEGATE TO REPUBLICAN NATIONAL CONVENTION',
    'ADD': 'ALT DELEGATE TO DEMOCRATIC NATIONAL CONVENTION',
    'ADR': 'ALT DELEGATE TO REPUBLICAN NATIONAL CONVENTION',
    'DSC': 'MEMBER OF DEMOCRATIC STATE COMMITTEE',
    'RSC': 'MEMBER OF REPUBLICAN STATE COMMITTEE'
}

#party code dictionary
party_code_dict = {
    'DEM': 'DEMOCRATIC',
    'REP': 'REPUBLICAN',
    'D/R': 'DEMOCRATIC/REPUBLICAN'
}


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
    """Read file content based on file extension and apply column names"""
    try:
        # Determine if this is a registration or returns file
        is_registration = 'registration' in filename.lower() or 'vrstat' in filename.lower()
        columns = registration_columns if is_registration else returns_columns
        expected_cols = len(columns)
        
        if filename.endswith('.xlsx'):
            df = pd.read_excel(BytesIO(response.content), header=None)
            if len(df.columns) != expected_cols:
                print(f"Warning: {filename} has {len(df.columns)} columns, expected {expected_cols}")
                df = pd.read_excel(BytesIO(response.content))
            if len(df.columns) == expected_cols:
                df.columns = columns
                
        elif filename.endswith('.txt'):
            # Print first few lines to debug
            content = StringIO(response.text)
            first_lines = [next(content) for _ in range(5)]
            print(f"\nFirst few lines of {filename}:")
            for line in first_lines:
                print(line.strip())
            
            # Reset content
            content = StringIO(response.text)
            
            # Try fixed width first
            df = pd.read_fwf(content, header=None)
            
            if len(df.columns) != expected_cols:
                # If fixed width fails, try delimiters
                content = StringIO(response.text)
                for delimiter in [',', '\t', '|', ';']:
                    try:
                        df = pd.read_csv(content, delimiter=delimiter, header=None)
                        if len(df.columns) == expected_cols:
                            break
                        content = StringIO(response.text)  # Reset for next try
                    except:
                        continue
            
            if len(df.columns) == expected_cols:
                df.columns = columns
            else:
                print(f"\nWarning: {filename} column count mismatch:")
                print(f"Found {len(df.columns)} columns: {df.columns.tolist()}")
                print(f"Expected {expected_cols} columns: {columns}")
                return None
                    
        elif filename.endswith('.csv'):
            for delimiter in [',', '\t', '|', ';']:
                try:
                    df = pd.read_csv(StringIO(response.text), delimiter=delimiter, header=None)
                    if len(df.columns) == expected_cols:
                        df.columns = columns
                        break
                except:
                    continue
            else:
                raise ValueError(f"Could not determine delimiter for {filename}")

        # Apply dictionary mappings
        if df is not None and len(df.columns) == expected_cols:
            df = process_codes(df, is_registration)
            return df
        else:
            print(f"Error: Could not properly read {filename}. Found {len(df.columns)} columns, expected {expected_cols}")
            return None

    except Exception as e:
        print(f"Error reading {filename}: {str(e)}")
        return None

def process_codes(df, is_registration):
    """Process code columns using the dictionary mappings"""
    try:
        # Common mappings for both types
        if 'county_code' in df.columns:
            df['county_name'] = df['county_code'].astype(str).str.zfill(2).map(county_code_dict)
        
        if 'municipality_type_code' in df.columns:
            df['municipality_type'] = df['municipality_type_code'].astype(str).map(municipality_type_code_dict)
            
        if 'election_type' in df.columns:
            df['election_type_name'] = df['election_type'].map(election_type_dict)
            
        # Returns specific mappings
        if not is_registration:
            if 'candidate_office_code' in df.columns:
                df['office_name'] = df['candidate_office_code'].map(office_code_dict)
                
        return df
        
    except Exception as e:
        print(f"Error processing codes: {str(e)}")
        return df

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






