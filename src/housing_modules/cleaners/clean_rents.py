#%%
import pandas as pd
import numpy as np
import re
from datetime import date, datetime

class DataCleaner():

    ALTRE_CARATTERISTICHE = ["Arredato", "Balcone", "Impianto tv", "Esposizione esterna", "Fibra ottica",
                             "Cancello elettrico", "Cantina", "Giardino comune", "Giardino privato",
                             "Impianto allarme", "Portiere", "Piscina"]
    TIPOLOGIE = ['villa', 'intera proprieta', 'appartamento', 'attico', 'loft', 'mansarda']
    TODAY = np.datetime64(date.today())
    FULL_CALENDAR = pd.DataFrame(pd.date_range(start="2023-01-01", end=TODAY), columns=['datetime'])

    def __init__(self, X):
        self.X = X

    def clean_data(self, X):

        # PREZZO
        X['prezzo'] = X['prezzo'].str.replace('€', '')
        X['prezzo'] = X['prezzo'].replace(r'[^0-9]', '', regex=True)
        X['prezzo'][X['prezzo'] == ''] = np.nan
        X['prezzo'] = X['prezzo'].astype(float)

        # DATETIME
        date_regex = r'(\d{2}/\d{2}/\d{4})'
        X['datetime'] = X['Riferimento e Data annuncio'].str.extract(date_regex)
        X['datetime'] = pd.to_datetime(X['datetime'], format='%d/%m/%Y')

        # superficie
        X['superficie'] = X['superficie'].replace(r'[^0-9]', '', regex=True)
        X['superficie'] = X['superficie'].str.replace('[^0-9\.]', '', regex=True)
        X['superficie'] = X['superficie'].astype(float)
        X.loc[X['superficie'] > 300, 'superficie'] = np.nan

        # POSTI AUTO
        X['posti auto'] = X['Posti Auto'].apply(lambda x: 0 if x == None else 1)

        # BAGNI PER STANZA
        X['bagni'] = X['bagni'].apply(lambda x: x if x else x)
        X['bagni'] = X['bagni'].apply(lambda x: pd.to_numeric(x, errors='coerce') )
        X['bagni'] = X['bagni'].astype(float)

        errors = X['stanze'].where(X['stanze'].str.contains('m'), np.nan).unique()
        X['stanze'] = X['stanze'].replace(errors, np.nan)
        X['stanze'] = X['stanze'].apply(lambda x: pd.to_numeric(x, errors='coerce') )
        X['stanze'] = X['stanze'].astype(float)

        X['bagni per stanza'] = X['bagni'] / X['stanze']

        # ULTIMO PIANO
        #piano
        X['piano'] = X['piano'].str.replace(r'\D', '')

        # totale pieni edificio
        X['totale piani edificio'] = X['totale piani edificio'].str.replace(r'\D', '')

        # ultimo piano
        X['ultimo piano'] = X['piano'] == X['totale piani edificio']
        X['ultimo piano'] = X['ultimo piano'].map({True: 1, False: 0})

        # energy class
        X['classe energetica'] = X['Efficienza energetica'].apply(lambda x: x[0] if x else np.nan)

        # tipologia
        for tipologia in self.TIPOLOGIE:
            X[tipologia] = X['tipologia'].apply(lambda x: tipologia in x.lower() if x else 0)
            X[tipologia] = X[tipologia].map({True: 1, False: 0})

        # stato
        X['stato'] = X['stato'].apply(lambda x: x.lower() if x else x)


        # riscaldamento
        #X['riscaldamento centralizzato'] = X['riscaldamento'].apply(lambda x: "centralizzato" in x.lower() if x else 0)
        X['riscaldamento centralizzato'] = X['riscaldamento'].str.contains("Centralizzato")
        X['riscaldamento centralizzato'] = X['riscaldamento centralizzato'].map({True: 1, False: 0})

        # vista mare
        X['vista mare'] = X['description'].str.contains("vista mare")
        X['vista mare'] = X['vista mare'].map({True: 1, False: 0})

        # ALTRE CARATTERISTICHE
        for char in self.ALTRE_CARATTERISTICHE:
            X[char] = X['altre caratteristiche'].apply(lambda x: char in x if x else 0)
            X[char] = X[char].map({True: 1, False: 0})

        final_columns = ['regione', 'citta', 'quartiere', 'prezzo', 'datetime', 'posti auto',
                         'bagni per stanza', 'bagni', 'stanze', 'ultimo piano', 'stato',
                         'classe energetica', 'vista mare', 'riscaldamento centralizzato',
                         'superficie']
        final_columns.extend(self.ALTRE_CARATTERISTICHE)
        final_columns.extend(self.TIPOLOGIE)

        df = X[final_columns]
        df.columns = [i.lower() for i in df.columns]

        return df


#%%
df = pd.read_parquet("dataframes/italy_housing_price_rent_raw.parquet.gzip")
cleaner = DataCleaner(df)
df_clean = cleaner.clean_data(df)

# %%
