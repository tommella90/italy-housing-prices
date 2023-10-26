#%%
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from termcolor import colored
import warnings 
warnings.filterwarnings("ignore")


class RealEstateScraper:
    def __init__(self, 
                 data_path='dataframes/rents_raw.parquet', 
                 location_focus='regione',
                 type_focus='vendita' # 'affitto'
                 ):
        self.data_path = data_path
        self.location_focus = location_focus
        self.type_focus = type_focus

    def read_parquet(self):
        try:
            return pd.read_parquet(self.data_path)
        except:
            df = pd.DataFrame()
            return df

    def get_downloaded_hrefs(self, df):
        try:
            hrefs = df['href'].tolist()
        except:
            hrefs = []
        return hrefs

    def get_all_webpages(self, limit, location):
        urls = []
        urls.append(f"https://www.immobiliare.it/{self.type_focus}-case/{location}/?criterio=rilevanza")
        for page in range(2, limit):
            url = f"https://www.immobiliare.it/{self.type_focus}-case/{location}/?criterio=rilevanza&pag={page}"
            urls.append(url)
        return urls

    def get_all_announcements_urls(self, all_pages, downloaded):
        all_announcements_urls = []

        for url in all_pages:
            try:
                response = requests.get(url)
                soup = bs(response.content, "html.parser")
                page_urls = soup.select(".in-reListCard__title")
                page_urls = [url.get("href") for url in page_urls]
                page_urls_new = [url for url in page_urls if url not in downloaded]
                all_announcements_urls.extend(page_urls_new)

            except:
                print(colored('ERROR in', 'red'))
                print(colored(url, 'red'))
                pass

        return all_announcements_urls

    def get_home_soup(self, url):
        response = requests.get(url)
        soup = bs(response.content)
        return soup, url

    def get_main_items(self, soup):
        main_items = soup.select(".nd-list__item.in-feat__item")
        items_label = ["prezzo", "stanze", "m2", "bagni", "piano"]
        items_value = [item.get_text() for item in main_items]
        d_items_main = dict(zip(items_label, items_value))
        return d_items_main

    def get_description(self, soup):
        description = soup.select(".in-readAll")
        description = {'descrizione': description[0].get_text()}
        return description

    def get_all_items(self, soup):
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

    def get_address(self, soup):
        address = soup.select(".in-location")
        address = [a.get_text() for a in address]
        location_id = ["citta", "quartiere", "via"]
        d_location = dict(zip(location_id, address))
        return d_location

    def make_dataframe(self, href):
        soup, url = self.get_home_soup(href)
        mergedDict = self.get_main_items(soup) | self.get_description(soup) | self.get_all_items(soup) | self.get_address(soup)
        mergedDict['href'] = url
        df = pd.DataFrame.from_dict(mergedDict, orient='index').T
        return df

    def main(self, n_pages, where):
        df_old = self.read_parquet()
        downloaded_hrefs = self.get_downloaded_hrefs(df_old)
        all_pages = self.get_all_webpages(n_pages, where)
        urls_found = self.get_all_announcements_urls(all_pages, downloaded_hrefs)
        existing_urls = list(df_old['href'])
        urls_to_scrape = list(set(urls_found).difference(set(existing_urls)))

        if len(urls_to_scrape) == 0:
            print(colored('No new data to scrape. Try tomorrow', 'yellow'))
            pass

        else:
            print(colored(f"Found {len(urls_to_scrape)} new announcements to scrape", 'green'))

        df_new = pd.DataFrame()
        for url in urls_to_scrape:
            try:
                new_row = self.make_dataframe(url)
                df_new = pd.concat([df_new, new_row], axis=0)

                if self.location_focus == "regione":
                     df_new['regione'] = where
                else:
                     df_new['regione'] = None

            except Exception as e:
                print(colored, ("ERROR : " + str(e), url, 'red'))

        print(colored(f"Saved {len(urls_to_scrape)} more announcements in {where}\n" , 'green', attrs=['bold']))

        return df_new

