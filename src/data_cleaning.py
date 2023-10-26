#%%
############
# CLEANING #
############
import pandas as pd
import housing_modules.cleaners.general_cleaner as cleaner
from termcolor import colored

#%%
# SALE
df_sales = pd.read_parquet('dataframes/sales_raw.parquet')
sale_cleaner_ = cleaner.DataCleaner(df_sales)
df_clean = sale_cleaner_.clean_data(df_sales)
df_clean = df_clean.reset_index(drop=True)
df_clean.to_parquet('dataframes/sales_clean.parquet')


# RENTS
df_rents = pd.read_parquet('dataframes/sales_raw.parquet')
rent_cleaner_ = cleaner.DataCleaner(df_rents)
df_clean = rent_cleaner_.clean_data(df_rents)
df_clean = df_clean.reset_index(drop=True)
df_clean.to_parquet('dataframes/rents_clean.parquet')

print('cleaning done')

#%%
pd.read_parquet("/Users/tommella90/source/italy-housing-prices/src/dataframes/sales_raw.parquet")


#%% 
df = pd.read_parquet('dataframes/rents_raw.parquet')

#%%
# pulire prezzo
df['prezzo'] = df['prezzo'].str.replace('€', '')
df['prezzo'] = df['prezzo'].replace(r'[^0-9]', '', regex=True)
df['prezzo'] = df['prezzo'].replace('', '0') 
df['prezzo'] = df['prezzo'].astype(float)

# data
date_regex = r'(\d{2}/\d{2}/\d{4})'
df['datetime'] = df['Riferimento e Data annuncio'].str.extract(date_regex)
df['datetime'] = pd.to_datetime(df['datetime'], format='%d/%m/%Y')

# superficie
df['superficie'] = df['superficie'].replace(r'[^0-9]', '', regex=True)
df['superficie'] = df['superficie'].str.replace('[^0-9\.]', '', regex=True)
df['superficie'] = df['superficie'].astype(float)

df['bagni'] = df['bagni'].apply(lambda x: x if x else x)
df['bagni'] = df['bagni'].apply(lambda x: pd.to_numeric(x, errors='coerce') )
df['bagni'] = df['bagni'].astype(int)

# %%
df.columns
# %%
df.to_parquet("dataframes/sales_raw2.parquet")
# %%
pd.read_parquet("dataframes/sales_raw2.parquet")
# %%
