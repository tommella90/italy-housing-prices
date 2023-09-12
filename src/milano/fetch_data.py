
#%%
import sys
modules_path = "/Users/tommella90/source/italy-house-prices"
sys.path.insert(0, modules_path)
import housing_modules.scrapers.scraping_sale_cities as sale_cities_scraper
import housing_modules.cleaners.clean_sales as sale_cleaner
import pandas as pd
import time
import tqdm

citta = "milano"

def update_dataframe(dataframe):
       try:
              df = pd.read_parquet(dataframe)
              print("searching data")
              new_data = sale_cities_scraper.main(n_pages, "milano")
              df_updated = pd.concat([df, new_data], axis=0)
              df_updated.to_parquet(dataframe)
       except:
            pass

# choose pages to scrape (max 80)
n_pages = int(input("How many pages do you want to scrape? (Max.:80)"))
start = time.time()

DATAFRAME_RENTS = "milano_sales.parquet"
df = pd.read_parquet(DATAFRAME_RENTS)
starting_n = len(df)
print("Starting n: ", starting_n)

update_dataframe(DATAFRAME_RENTS)

#%%
import housing_modules.cleaners.clean_sales as sale_cleaner

print("clean data")
cleaner = sale_cleaner.DataCleaner("milano_sales.parquet")
 
df = cleaner.clean_data()
#%%
df_clean = cleaner()

print('done')

# %%

