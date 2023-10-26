#%%
import pandas as pd
import housing_modules.scrapers.scraping_rents_cities as rents_cities_scraper
import housing_modules.scrapers.scraping_rents_regions as rents_regions_scraper
import housing_modules.scrapers.scraping_sale_cities as sale_cities_scraper
import housing_modules.scrapers.scraping_sale_regions as sale_regions_scraper
#import credential_config.get_credentials as gc
import subprocess
import time
import os 
from tqdm import tqdm
from termcolor import colored
import warnings
warnings.filterwarnings("ignore")

REGIONI = ['lombardia', 'piemonte', 'veneto', 'emilia-romagna', 'toscana', 'lazio',
           'campania', 'sicilia', 'sardegna', 'puglia', 'abruzzo', 'marche', 'liguria', 
           'calabria' 'friuli-venezia-giulia', 'trentino-alto-adige', 'umbria', 'molise',
           'basilicata', 'valle-d-aosta']


CAPOLUOGHI = ['milano', 'torino', 'genova', 'bologna', 'firenze', 'roma', 'napoli', 'aosta', 
              'palermo', 'cagliari', 'bari', 'ancona', 'bologna', 'cagliari', 'trento', 
              'campobasso', 'catanzaro', 'genoa', 'l-aquila', 'potenza', 'trieste', 'venezia']


def print_process(text):
    print(colored("_"*20, 'blue', attrs=['bold']))
    print(colored(f"{text}- regions", 'blue', attrs=['bold']))
    print(colored("_"*20, 'blue', attrs=['bold']))
    print(colored(f"Fetching the urls...", 'blue', attrs=['bold']))
    print("\n\n")


def update_dataframe(dataframe, area, scraper, progress):
    for index, regione in tqdm(enumerate(area), total=len(area), desc=progress):
        print("*"*20)
        try:
            df = pd.read_parquet(dataframe)
            print("*"*index)
            print(colored(f"{regione.upper()}, {index+1}/{len(area)}", "green"))
            if scraper == "rents_region":
               new_data = rents_regions_scraper.main(n_pages, regione)
            elif scraper == "rents_city":
               new_data = rents_cities_scraper.main(n_pages, regione)
            elif scraper == "sales_region":
               new_data = sale_regions_scraper.main(n_pages, regione)
            elif scraper == "sales_city":
               new_data = sale_cities_scraper.main(n_pages, regione)

            df_updated = pd.concat([df, new_data], axis=0)
            df_updated.to_parquet(dataframe)
        except:
            pass
    

# choose pages to scrape (max 80)
n_pages = int(input("How many pages do you want to scrape? (Max.:80)"))
start = time.time()


# ----- #
# RENTS #
# ----- #
DATAFRAME_RENTS = "src/dataframes/rents_raw.parquet"
df = pd.read_parquet(DATAFRAME_RENTS)
starting_n = len(df)
print("Starting n: ", starting_n)


# regions
SCRAPER = "rents_region"
print_process("1. RENT - regions")
update_dataframe(DATAFRAME_RENTS, REGIONI, SCRAPER, "Rents -regions: 1/4")

# cities
SCRAPER = "rents_cities"
print_process("2. RENT - cities")
update_dataframe(DATAFRAME_RENTS, CAPOLUOGHI, SCRAPER, "Rents - cities: 2/4")

# ----- #
# SALES #
# ----- #
DATAFRAME_SALES = "src/dataframes/sales_raw.parquet"
df = pd.read_parquet(DATAFRAME_SALES)
starting_n = len(df)
print("Starting n: ", starting_n)

# regions
SCRAPER = "sales_regions"
print_process("3. SALES - regions")
update_dataframe(DATAFRAME_SALES, REGIONI, SCRAPER, "Sales - regions: 3/4")

# cities
SCRAPER = "sales_cities"
print_process("4. SALES - cities")
update_dataframe(DATAFRAME_SALES, CAPOLUOGHI, SCRAPER, "Sales - cities: 4/4")


# done
time = time.time() - start
print("\n\n")
print(f'process terminated in {time}')



# Close
key = input("Press any key to close...")


# %%
