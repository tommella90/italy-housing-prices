import pandas as pd
import numpy as np

def load_data():
    df = pd.read_parquet("../dataframes/italy_housing_price_rent_raw.parquet.gzip")
    return df

def clean_data(df):
    df['regione'] = df['regione'].str.title()
    df['citta'] = df['citta'].str.title()
    df = df.dropna(subset=['regione', 'citta'])
    df = df[['citta', 'quartiere']]
    return df


from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="Google Maps")

df = load_data()
df = clean_data(df)

#%%
provinces = pd.read_excel('../data/province-italiane.xlsx')
provinces = list(provinces['Provincia'])

#%%
import json

for prov in provinces:
    neighbourhoods = df.loc[df['citta']==prov]
    neighbourhoods = list(neighbourhoods['quartiere'].unique())
    neighbourhoods.extend([prov])

    coords = {}

    for n in neighbourhoods:
        try:
            location = geolocator.geocode(n)
            coords[n] = (location.latitude, location.longitude)
        except:
            pass

    with open(f'../data/prov_coords/{prov}.json', 'w') as fp:
        json.dump(coords, fp)

    print(prov)

#%%
geolocator = Nominatim(user_agent="Google Maps")
geocode = lambda query: geolocator.geocode("%s, Liguria" % query)

location = geolocator.geocode("Carignano")
#coords = (location.latitude, location.longitude)



#%%
