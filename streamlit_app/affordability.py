import pandas as pd
import numpy as np
from datetime import date, datetime
import plotly.express as px
import streamlit as st

class Affordability():

    TODAY = np.datetime64(date.today())
    FULL_CALENDAR = pd.DataFrame(pd.date_range(start="2023-01-01", end=TODAY), columns=['datetime'])
    REGIONI = (
        'Abruzzo', 'Basilicata', 'Calabria', 'Campania', 'Emilia-Romagna',
        'Friuli-Venezia Giulia', 'Lazio', 'Liguria', 'Lombardia', 'Marche',
        'Molise', 'Piemonte', 'Puglia', 'Sardegna', 'Sicilia', 'Toscana',
        'Veneto', 'Valle-D-Aosta', 'Trentino-Alto-Adige'
    )
    CAPOLUOGHI = (
        "Aosta", "Torino", "Genova", "Milano", "Trento", "Venezia", "Trieste",
        "Bologna", "Firenze", "Perugia", "Ancona", "Roma", "L'Aquila",
        "Campobasso", "Napoli", "Bari", "Potenza", "Catanzaro", "Palermo", "Cagliari"
    )

    COLUMNS = ["superficie", "bagni", "stanze", "bagni per stanza", "posti auto",
               "ultimo piano", "vista mare", "riscaldamento centralizzato", "balcone",
               "esposizione esterna", "cantina", "giardino comune", "giardino privato", "piscina"]

    TYPES = ['villa', 'intera proprieta', 'appartamento', 'attico', 'loft']


    def __init__(self,
                 df_path="dataframes/italy_housing_price_rent_raw.parquet.gzip",
                 provinces_path="geo_data/province-italiane.xlsx"):
        self.df_path = df_path
        self.provinces_path = provinces_path

    def side_bar_city(self):
        st.sidebar.header("PROVINCIA")
        default_province = ('Milano', 'Genova')
        select_province = st.sidebar.selectbox(
            "Choose a province", self.CAPOLUOGHI,
        )
        return select_province

    def sidebar_multiselect_type(self):
        categories = ['all types', 'appartamento', 'intera proprieta', 'villa', 'attico', 'loft']
        selected_type = st.sidebar.selectbox("TIPOLOGIA", categories)
        return selected_type

    def select_type(self, df, selected_type):
        if selected_type == 'all types':
            return df
        else:
            df = df.loc[df[selected_type]==1]
        return df

    def select_time_range(self, df, time_start, time_end):
        df = df.loc[(df['datetime'] >= time_start) & (df['datetime'] <= time_end)]
        return df

    def select_price_range(self, df, min_price, max_price):
        df = df.loc[(df['prezzo'] >= min_price) & (df['prezzo'] <= max_price)]
        return df

    def sidebar_select_time_range(self):
        today_string = np.datetime_as_string(self.TODAY, unit='D')
        start_time = datetime.strptime("2023-01-01", "%Y-%m-%d")
        end_time = datetime.strptime(today_string, "%Y-%m-%d")
        date_values = st.slider('SELECT A DATE RANGE',
                                min_value=start_time,
                                max_value=end_time,
                                value=(start_time, end_time),
                                format="YYYY-MM-DD")
        return date_values[0], date_values[1]

    def slider_price_limit(self):
        price_limit = st.slider('Select a maximum price',
                                min_value=0, max_value=10000,
                                value=(500, 800))
        return price_limit[0], price_limit[1]

    def side_bar_price_range(self):
        df = pd.read_parquet(self.df_path)
        max_value = int(round(df['prezzo'].max()))
        price_ranges_city = st.slider('SELECT A PRICE RANGE',
                                      key="price_range",
                                      min_value=0, max_value=max_value,
                                      value=(500, 800))
        st.write('You selected:', price_ranges_city)
        return price_ranges_city[0], price_ranges_city[1]

    def groupby_city(self, df, min_price, max_price):

        df = df.loc[df['citta'].isin(self.CAPOLUOGHI)]
        df = df.loc[(df['prezzo'] >= min_price) & (df['prezzo'] <= max_price)]

        values = df.groupby(["citta"]).agg({
            "superficie": "mean",
            "bagni": "mean",
            "stanze": "mean",
            "bagni per stanza": "mean",
            "posti auto": "mean",
            "ultimo piano": "mean",
            "vista mare": "mean",
            "riscaldamento centralizzato": "mean",
            "balcone": "mean",
            "esposizione esterna": "mean",
            "cantina": "mean",
            "giardino comune": "mean",
            "giardino privato": "mean",
            "piscina": "mean",
        })

        # classe energetica, stato, arredato
        energy_class = df[['citta', 'classe energetica']].groupby("citta").value_counts().unstack()

        superficie = values['superficie'].reset_index().sort_values(by='superficie', ascending=False)
        bagni = values['bagni'].reset_index().sort_values(by='bagni', ascending=False)
        stanze = values['stanze'].reset_index().sort_values(by='stanze', ascending=False)
        bagni_per_stanza = values['bagni per stanza'].reset_index().sort_values(by='bagni per stanza', ascending=False)

        categories = values[['posti auto', 'ultimo piano', 'vista mare', 'riscaldamento centralizzato', 'balcone',
                             'esposizione esterna', 'cantina', 'giardino comune', 'giardino privato', 'piscina']]
        categories = round(categories*100, 3)
        return values, superficie, bagni, stanze, bagni_per_stanza, categories, energy_class

    def sidebar_select_column(self):
        st.sidebar.header("CHOOSE A COLUMN")
        columns = list(self.COLUMNS)
        columns_selected = st.sidebar.selectbox("OPTIONS:", columns)
        return columns_selected

    def plot_values(self, values, column_name):
        values = values[column_name].reset_index().sort_values(by=column_name, ascending=False)

        fig = px.bar(values, x=values[column_name], y=values['citta'],
                     orientation='h', color=column_name,
                     color_continuous_scale='turbo',)
        fig.update_layout(
            title=f"{column_name.upper()}",
            xaxis_title=f"{column_name.upper()}",
            yaxis_title="CITY",
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
            )
        )
        st.plotly_chart(fig)

    def heatmap_categories(self, categories):
        categories = categories.T
        fig = px.imshow(categories, color_continuous_scale='blues')
        fig.update_layout(
            title="HOUSE CHARACTERISTICS IN %",
            xaxis_title="% of presence",
            yaxis_title="CITY",
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
            )
        )
        st.plotly_chart(fig)

    def main(self, df, selected_types,
             time_start='2023-01-01', time_end=TODAY,
             min_price=500, max_price=800,
             column_name='superficie'):

        df_sliced = self.select_time_range(df, time_start, time_end)
        df_sliced = self.select_type(df_sliced, selected_types)
        values, superficie, bagni, stanze, bagni_per_stanza, categories, energy_class = self.groupby_city(df_sliced, min_price, max_price)

        try:
            self.plot_values(values, column_name)
        except:
            st.write("Not enough data on this category ")

        self.heatmap_categories(categories)
        st.table(values)


#%%
