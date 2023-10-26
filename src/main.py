#%%
import pandas as pd
from housing_modules.scraping import RealEstateScraper as scraper
from tqdm import tqdm
from termcolor import colored
import logging
import warnings
import time
warnings.filterwarnings("ignore")

n_pages = 3

# import os
# set working directory

# LOCATION 
regione = ['lombardia', 'piemonte', 'veneto', 'emilia-romagna', 'toscana', 'lazio', 'campania', 'sicilia', 'sardegna', 'puglia', 'abruzzo', 'marche', 'liguria', 'calabria' 'friuli-venezia-giulia', 'trentino-alto-adige', 'umbria', 'molise','basilicata', 'valle-d-aosta']


citta = ['milano', 'torino', 'genova', 'bologna', 'firenze', 'roma', 'napoli', 'aosta', 'palermo', 'cagliari', 'bari', 'ancona', 'bologna', 'cagliari', 'trento', 'campobasso', 'catanzaro', 'l-aquila', 'potenza', 'trieste', 'venezia']
#%%

# TRACK ERRORS
log_file = 'scraper_errors.log'
logging.basicConfig(filename=log_file, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


def print_process(text):
    print(colored("_"*20, 'blue', attrs=['bold']))
    print(colored(f"{text}- regions", 'blue', attrs=['bold']))
    print(colored("_"*20, 'blue', attrs=['bold']))
    print(colored(f"Fetching the urls...", 'blue', attrs=['bold']))
    print("\n\n")


def scrape(scraper, location, n_pages=2):
    try:
        df = pd.read_parquet(DATA)
        print(colored(f"{location.upper()}", "green"))
        df_new = scraper.main(n_pages=n_pages, where=location)
        df_new = pd.concat([df, df_new], axis=0)
        df_new.to_parquet('src/dataframes/sales_raw.parquet')
        
    except Exception as e:
        error_message = str(e)
        print(colored(error_message, "red"))
        # Log the error to the file
        logging.error(f"Error in {location}: {error_message}")


start = time.time()

## SALES
DATA = "src/dataframes/sales_raw.parquet"
#df = pd.read_parquet(DATA)



scraper_sale_region = scraper(data_path=DATA,
                            location_focus='regione',
                            type_focus='vendita' 
                            )

for regione_item in tqdm(regione, total=len(regione), desc="Sales - regions"):
    print("*" * 20)
    scrape(scraper_sale_region, regione_item, n_pages=n_pages)


scraper_sale_city = scraper(data_path=DATA,
                              location_focus='citta',
                              type_focus='vendita'
                              )

for citta_item in tqdm(citta, total=len(citta), desc="Sales - cities"):
    print("*" * 20)
    scrape(scraper_sale_city, citta_item, n_pages=n_pages)


## RENTS
DATA = "src/dataframes/rents_raw.parquet"
scraper_rent_region = scraper(data_path=DATA,
                            location_focus='regione',
                            type_focus='affitto'
                            )

for regione_item in tqdm(regione, total=len(regione), desc="Rents - regions"):
    print("*" * 20)
    scrape(scraper_rent_region, regione_item, n_pages=n_pages)


scraper_rent_city = scraper(data_path=DATA,
                            location_focus='citta',
                            type_focus='affitto'
                            )

for citta_item in tqdm(citta, total=len(citta), desc="Rents - cities"):
      print("*" * 20)
      scrape(scraper_rent_city, citta_item, n_pages=n_pages)


# TIME calculate time of execution
minutes = (time.time() - start)/60
print("ended in ", minutes, " minutes")
print("\n\n")


# %%
