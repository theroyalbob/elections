import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# load a parquet file
df = pd.read_parquet('bulk_data/registration/2024/primary_voterregistration_2024_primary_precinct.parquet')

# print the first 5 rows of the dataframe
print(df.columns)
