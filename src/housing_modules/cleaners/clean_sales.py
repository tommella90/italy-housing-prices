import pandas as pd
import numpy as np
import re
from datetime import date, datetime


class DataCleaner:

    ALTRE_CARATTERISTICHE = ["Arredato", "Balcone", "Impianto tv", "Esposizione esterna", "Fibra ottica", "Cancello elettrico", "Cantina", "Giardino comune", "Giardino privato", "Impianto allarme", "Portiere", "Piscina"]
    
    TIPOLOGIE = ['villa', 'intera proprieta', 'appartamento', 'attico', 'loft', 'mansarda']

    TODAY = np.datetime64(date.today())
    FULL_CALENDAR = pd.DataFrame(pd.date_range(start="2023-01-01", end=TODAY), columns=['datetime'])

    def __init__(self, filepath):
        self.filepath = filepath
        self.df = None

    def clean_data(self, X):

        # PREZZO
        df['prezzo'] = df['prezzo'].str.replace('€', '')
        df['prezzo'] = df['prezzo'].replace(r'[^0-9]', '', regex=True)
        df['prezzo'][df['prezzo'] == ''] = np.nan
        df['prezzo'] = df['prezzo'].astype(float)

        # DATETIME
        date_regex = r'(\d{2}/\d{2}/\d{4})'
        df['datetime'] = df['Riferimento e Data annuncio'].str.extract(date_regex)
        df['datetime'] = pd.to_datetime(df['datetime'], format='%d/%m/%Y')

        # superficie
        df['superficie'] = df['superficie'].replace(r'[^0-9]', '', regex=True)
        df['superficie'] = df['superficie'].str.replace('[^0-9\.]', '', regex=True)
        df['superficie'] = df['superficie'].astype(float)
        X.loc[df['superficie'] > 300, 'superficie'] = np.nan

        # POSTI AUTO
        df['posti auto'] = df['Posti Auto'].apply(lambda x: 0 if x == None else 1)

        # BAGNI PER STANZA
        df['bagni'] = df['bagni'].apply(lambda x: df[0] if x else x)
        df['bagni'] = df['bagni'].apply(lambda x: pd.to_numeric(x, errors='coerce') )
        df['bagni'] = df['bagni'].astype(float)

        errors = df['stanze'].where(df['stanze'].str.contains('m'), np.nan).unique()
        df['stanze'] = df['stanze'].replace(errors, np.nan)
        df['stanze'] = df['stanze'].apply(lambda x: pd.to_numeric(x, errors='coerce') )
        df['stanze'] = df['stanze'].astype(float)

        df['bagni per stanza'] = df['bagni'] / df['stanze']

        # ULTIMO PIANO
        #piano
        df['piano'] = df['piano'].str.replace(r'\D', '')

        # totale pieni edificio
        df['totale piani edificio'] = df['totale piani edificio'].str.replace(r'\D', '')

        # ultimo piano
        df['ultimo piano'] = df['piano'] == df['totale piani edificio']
        df['ultimo piano'] = df['ultimo piano'].map({True: 1, False: 0})

        # energy class
        df['classe energetica'] = df['Efficienza energetica'].apply(lambda x: df[0] if x else np.nan)

        # tipologia
        for tipologia in self.TIPOLOGIE:
            df[tipologia] = df['tipologia'].apply(lambda x: tipologia in x.lower() if x else 0)
            df[tipologia] = df[tipologia].map({True: 1, False: 0})

        # stato
        df['stato'] = df['stato'].apply(lambda x: x.lower() if x else x)

        # riscaldamento
        df['riscaldamento centralizzato'] = df['riscaldamento'].str.contains("Centralizzato")
        df['riscaldamento centralizzato'] = df['riscaldamento centralizzato'].map({True: 1, False: 0})

        # vista mare
        df['vista mare'] = df['description'].str.contains("vista mare")
        df['vista mare'] = df['vista mare'].map({True: 1, False: 0})

        # ALTRE CARATTERISTICHE
        for char in self.ALTRE_CARATTERISTICHE:
            df[char] = df['altre caratteristiche'].apply(lambda x: char in x if x else 0)
            df[char] = df[char].map({True: 1, False: 0})

        final_columns = ['regione', 'citta', 'quartiere', 'prezzo', 'datetime', 'posti auto','bagni per stanza', 'bagni', 'stanze', 'ultimo piano', 'stato',
        'classe energetica', 'vista mare', 'riscaldamento centralizzato', 'superficie']
        final_columns.extend(self.ALTRE_CARATTERISTICHE)
        final_columns.extend(self.TIPOLOGIE)

        df = df[final_columns]
        df.columns = [i.lower() for i in df.columns]

        return df
