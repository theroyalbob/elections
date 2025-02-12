import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# load a parquet file
df = pd.read_parquet('bulk_data/returns/2015/primary_ElectionReturns_2015_Primary_PrecinctReturns.parquet', engine='pyarrow')

# print the first 5 rows of the dataframe
print(df.columns)
