#%%
############
# CLEANING #
############
import pandas as pd
import housing_modules.cleaners.general_cleaner as cleaner
from termcolor import colored

# SALE
df_sales = pd.read_parquet('dataframes/sales_raw.parquet')
sale_cleaner_ = cleaner.DataCleaner(df_sales)
df_clean = sale_cleaner_.clean_data(df_sales)
df_clean = df_clean.reset_index(drop=True)
df_clean.to_parquet('dataframes/sales_clean.parquet')


# RENTS
df_rents = pd.read_parquet('dataframes/rents_raw.parquet')
rent_cleaner_ = cleaner.DataCleaner(df_rents)
df_clean = rent_cleaner_.clean_data(df_rents)
df_clean = df_clean.reset_index(drop=True)
df_clean.to_parquet('dataframes/rents_clean.parquet')

print('cleaning done')

