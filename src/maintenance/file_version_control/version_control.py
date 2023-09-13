#%%
import pandas as pd
import os

version = len(os.listdir('rents'))
df = pd.read_parquet('../dataframes/italy_housing_price_rent_raw.parquet.gzip', engine='fastparquet')
df.to_parquet(f'rents/italy_housing_rents_{version+1}.parquet.gzip', compression='gzip')


version = len(os.listdir('sales'))
df = pd.read_parquet('../dataframes/italy_housing_price_sale_raw.parquet.gzip', engine='fastparquet')
df.to_parquet(f'italy_housing_sales_{version+1}.parquet.gzip', compression='gzip')

print('done')
