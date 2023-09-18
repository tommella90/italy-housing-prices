#%%
from bs4 import BeautifulSoup as bs
import numpy as np
import pandas as pd
from urllib import *
import requests
import time
import random
from termcolor import colored
import warnings
warnings.filterwarnings("ignore")

capoluoghi = {
    "milano": "lombardia", "torino": "piemonte", "genova": "liguria", 
    "bologna": "emilia-Romagna", "firenze": "toscana", "roma": "lazio",
    "napoli": "campania", "aosta": "valle-d-aosta", "palermo": "sicilia",
    "cagliari": "sardegna", "bari": "puglia", "ancona": "marche",
    "trento": "trentino-alto-adige", "campobasso": "molise",
    "catanzaro": "calabria", "l-aquila": "abruzzo", "potenza": "basilicata", "trieste": "friuli-venezia-giulia", "venezia": "veneto"
    }

def read_parquet():
    try:
        dataframe = "dataframes/italy_housing_price_rent_raw.parquet.gzip"
        return pd.read_parquet(dataframe)
    except:
        df = pd.DataFrame()
        return df


def get_downloaded_hrefs(df):
    try:
        hrefs = df['href'].tolist()
    except:
        hrefs = []
    return hrefs


def get_all_webpages(limit, citta):
    urls = []
    urls.append(f"https://www.immobiliare.it/affitto-case/{citta}/?criterio=dataModifica&ordine=desc")
    for page in range(2, limit):
        url = f"https://www.immobiliare.it/affitto-case/{citta}/?criterio=dataModifica&ordine=desc&pag={page}"
        urls.append(url)
    return urls


def get_all_announcements_urls(all_pages, downloaded):
    all_announcements_urls = []

    for index, url in enumerate(all_pages):
        try:
            if index % 10 == 0:
                print("Page: ", index, " of ", len(all_pages))
            response = requests.get(url)
            soup = bs(response.content, "html.parser")
            page_urls = soup.select(".in-card__title")
            page_urls = [url.get("href") for url in page_urls]
            page_urls_new = [url for url in page_urls if url not in downloaded]
            all_announcements_urls.append(page_urls_new)

        except:
            print(colored('ERROR in', 'red'))
            print(colored(url, 'red'))
            pass

    all_announcements_flat = [item for sublist in all_announcements_urls for item in sublist]
    return all_announcements_flat


# GO TO EACH ANNOUNCEMENT AND GET INFO
def get_home_soup(url):
    response = requests.get(url)
    soup = bs(response.content)
    return soup, url


# GET PRICE
def get_price(soup):
    div = soup.select(".nd-list__item.in-feat__item.in-feat__item--main")
    return div[0].get_text()


# GET INFORMATION ABOUT THE
def get_main_items(soup):
    main_items = soup.select(".nd-list__item.in-feat__item")
    items_label = ["prezzo", "stanze", "m2", "bagni", "piano"]
    items_value = [item.get_text() for item in main_items]
    d_items_main = dict(zip(items_label, items_value))
    return d_items_main

def get_description(soup):
    description = soup.select(".in-readAll")
    description = {'description': description[0].get_text()}
    return description


# GET ALL ITEMS
def get_characteristics(soup):
    caratteristiche = soup.select(".in-realEstateFeatures__list")
    titles_class = caratteristiche[0].select(".in-realEstateFeatures__title")
    titles = [item.get_text() for item in titles_class]
    values_class = caratteristiche[0].select(".in-realEstateFeatures__value")
    values = [item.get_text() for item in values_class]

    caretteristiche_dict = dict(zip(titles, values))
    if "altre caratteristiche" in caretteristiche_dict:
        del caretteristiche_dict["altre caratteristiche"]

    other_characteristics = values_class[-1]
    other_characteristics_values = [item.get_text() for item in other_characteristics]
    caretteristiche_dict["altre caratteristiche"] = other_characteristics_values
    return caretteristiche_dict
    
def get_costs(soup):
    caratteristiche = soup.select(".in-realEstateFeatures__list")
    costi_titles = caratteristiche[1].select(".in-realEstateFeatures__title")
    costi_titles_text = [item.get_text() for item in costi_titles]
    costi_values = caratteristiche[1].select(".in-realEstateFeatures__value")
    costi_values_text = [item.get_text() for item in costi_values]
    costi_dict = dict(zip(costi_titles_text, costi_values_text))
    return costi_dict

def get_energy_efficiency(soup):
    caratteristiche = soup.select(".in-realEstateFeatures__list")
    energy_titles = caratteristiche[2].select(".in-realEstateFeatures__title")
    energy_titles_text = [item.get_text() for item in energy_titles]
    energy_values = caratteristiche[2].select(".in-realEstateFeatures__value")
    energy_values_text = [item.get_text() for item in energy_values]
    energy_dict = dict(zip(energy_titles_text, energy_values_text))
    return energy_dict
    
    
# ADDRESS
def get_address(soup):
    address = soup.select(".in-location")
    address = [a.get_text() for a in address]
    location_id = ["citta", "quartiere", "via"]
    d_location = dict(zip(location_id, address))
    return d_location


# CREATE PANDAS DATAFRAME
def make_dataframe(href):
    soup, url = get_home_soup(href)

    elements_constructor = [get_main_items , get_description , get_characteristics , \
         get_costs , get_energy_efficiency , get_address]

    dicts = []
    for item in elements_constructor:
        try:
            dicts.append(item(soup))
        except:
            pass

    all_information = {}
    for index, element in enumerate(dicts):
        try:
            all_information.update(element)
        except:
            pass

    #mergedDict = get_main_items(soup) | get_description(soup) | get_characteristics(soup) | \
    #             get_costs(soup) | get_energy_efficiency(soup) | get_address(soup)

    df = pd.DataFrame.from_dict(all_information, orient='index').T
    df['href'] = url
    return df


def find_new_announcements(downloaded_hrefs, all_announcements_urls):
    diff = list(set(all_announcements_urls).difference(set(downloaded_hrefs)))
    return diff


def main(limit, citta):
    sleep = random.randint(1, 10)/10

    df_old = read_parquet()
    downloaded_hrefs = get_downloaded_hrefs(df_old)
    all_pages = get_all_webpages(limit, citta)
    urls_to_scrape = get_all_announcements_urls(all_pages, downloaded_hrefs)
    new_urls = find_new_announcements(df_old, urls_to_scrape)

    if len(new_urls)==0:
        print(colored('No new data to scrape. Try tomorrow', 'yellow'))
        pass

    else:
        print(colored(f"Found {len(new_urls)} new announcements to scrape", 'green'))
        print(colored(f"Creating the new data...", 'blue', attrs=['bold']))

        df_new = pd.DataFrame()
        for index, url in enumerate(new_urls):
            if index % 100 == 0:
                print(index, "/", len(new_urls))

            try:
                new_row = make_dataframe(url)
                new_row['regione'] = capoluoghi[new_row['citta'][0].lower()]
                df_new = pd.concat([df_new, new_row], axis=0)
                #time.sleep(sleep)
                
            except Exception as e:
                print("ERROR : "+str(e), url)

        #df_updated.to_parquet("dataframes/italy_housing_price_rent_raw.parquet.gzip", compression='gzip')
        print(colored(f"Saved {len(new_urls)} more annoucements\n", 'green', attrs=['bold']))

        return df_new

