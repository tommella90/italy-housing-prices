#%%
from bs4 import BeautifulSoup as bs
import pandas as pd
from urllib import *
import requests
import time
import random
from termcolor import colored
import warnings

import re



warnings.filterwarnings("ignore")

def read_parquet():
    try:
        return pd.read_parquet('italy_housing_price_sale_raw.parquet.gzip')
    except:
        df = pd.DataFrame()
        return df


def get_downloaded_hrefs(df):
    try:
        hrefs = df['href'].tolist()
    except:
        hrefs = []
    return hrefs


def get_all_webpages(limit, regione):
    urls = []
    urls.append(f"https://www.immobiliare.it/vendita-case/{regione}/?criterio=rilevanza")
    for page in range(2, limit):
        url = f"https://www.immobiliare.it/vendita-case/{regione}/?criterio=rilevanza&pag={page}"
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
def get_all_items(soup):
    caretteristiche = soup.select(".in-realEstateFeatures__list")
    titles_class = caretteristiche[0].select(".in-realEstateFeatures__title")
    titles = [item.get_text() for item in titles_class]
    values_class = caretteristiche[0].select(".in-realEstateFeatures__value")
    values = [item.get_text() for item in values_class]

    caretteristiche_dict = dict(zip(titles, values))
    if "altre caratteristiche" in caretteristiche_dict:
        del caretteristiche_dict["altre caratteristiche"]

    other_characteristics = values_class[-1]
    other_characteristics_values = [item.get_text() for item in other_characteristics]
    caretteristiche_dict["other_characteristics"] = other_characteristics_values

    return caretteristiche_dict


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
    mergedDict = get_main_items(soup) | get_description(soup) | get_all_items(soup) | get_address(soup)
    df = pd.DataFrame.from_dict(mergedDict, orient='index').T
    df['href'] = url
    return df


def find_new_announcements(downloaded_hrefs, all_announcements_urls):
    diff = list(set(all_announcements_urls).difference(set(downloaded_hrefs)))
    return diff


def main(limit, regione):
    sleep = random.randint(1, 10)/10

    df_old = read_parquet()
    downloaded_hrefs = get_downloaded_hrefs(df_old)
    all_pages = get_all_webpages(limit, regione)
    urls_to_scrape = get_all_announcements_urls(all_pages, downloaded_hrefs)
    new_urls = find_new_announcements(df_old, urls_to_scrape)

    if len(new_urls)==0:
        print(colored('No new data to scrape. Try tomorrow', 'yellow'))
        pass

    else:
        print(colored(f"Found {len(new_urls)} new announcements to scrape", 'green'))

        df_new = pd.DataFrame()
        for index, url in enumerate(new_urls):
            if index % 100 == 0:
                print(index, "/", len(new_urls))

            try:
                new_row = make_dataframe(url)
                df_new = pd.concat([df_new, new_row], axis=0)
                df_new['regione'] = regione
                #time.sleep(sleep)
            except Exception as e:
                print("ERROR : "+str(e), url)

    print(colored(f"Saved {len(new_urls)} more annoucements\n", 'green', attrs=['bold']))

    return df_new


# %%
limit = 1
regione = "lombardia"

df_old = pd.read_parquet('dataframes/sales_raw.parquet')
downloaded_hrefs = get_downloaded_hrefs(df_old)
all_pages = get_all_webpages(limit, regione)
urls_to_scrape = get_all_announcements_urls(all_pages, downloaded_hrefs)
new_urls = find_new_announcements(df_old, urls_to_scrape)


# %%
new_urls
# %%
df_old.describe()
# %%

import os
os.listdir()
# %%
