import pandas as pd
import numpy as np
from datetime import date, datetime
import plotly.express as px
import streamlit as st

class PriceTimeSeries:

    TODAY = np.datetime64(date.today())
    FULL_CALENDAR = pd.DataFrame(pd.date_range(start="2023-01-01", end=TODAY), columns=['datetime'])
    REGIONI = (
           'Abruzzo', 'Basilicata', 'Calabria', 'Campania', 'Emilia-Romagna',
           'Friuli-Venezia Giulia', 'Lazio', 'Liguria', 'Lombardia', 'Marche',
           'Molise', 'Piemonte', 'Puglia', 'Sardegna', 'Sicilia', 'Toscana',
           'Veneto', 'Valle-D-Aosta', 'Trentino-Alto-Adige'
           )

    def __init__(self,
                 df_path="dataframes/italy_housing_price_rent_clean.parquet.gzip"):
        self.df_path = df_path

    def load_data(self):
        return pd.read_parquet(self.df_path)

    def clean_datetime(self, df):
        df = df.loc[(df['datetime'] > '2023-01-01') & (df['datetime'] < self.TODAY)]
        df = pd.merge(df, self.FULL_CALENDAR, how='outer', on='datetime')
        return df

    def select_time_range(self, df, time_start, time_end):
        df = df.loc[(df['datetime'] >= time_start) & (df['datetime'] <= time_end)]
        return df

    def get_week_and_month(self, df):
        df['week'] = df['datetime'].dt.to_period('W').dt.start_time
        df['month'] = df['datetime'].dt.to_period('M').dt.start_time
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

    def clean_data(self, df, time_start='2023-01-01', time_end=TODAY):
        df = self.clean_datetime(df)
        df = self.get_week_and_month(df)
        df = self.select_time_range(df, time_start, time_end)
        return df

    def sidebar_select_seasonality(self):
        seasonality = st.sidebar.selectbox('Select a period', ['day', 'week', 'month'])
        if seasonality == 'day':
            seasonality = 'datetime'
        return seasonality

    def slider_price_limit(self):
        price_limit = st.slider('Select a maximum price',
                                min_value=0, max_value=20000,
                                value=(0, 5000))
        return price_limit[1]

    def plot_time_series(self, df, period, title, area=None):
        fig = px.line(df, x=period, y='prezzo',
                      color=area,
                      hover_data=[area],
                      color_discrete_sequence=px.colors.qualitative.Pastel,
                      template="plotly_white",
                      width=1000, height=600,
                      )
        fig.update_layout(xaxis_title="Date", yaxis_title="Price (euros)",
                          title={'text': title,'font': {'size': 24} }
                          )
        st.plotly_chart(fig)

    def plot_average_italy(self, df, period, max_price=5000):
        avg_italy = self.get_week_and_month(df)
        avg_italy = avg_italy.groupby([period])['prezzo'].mean().reset_index()
        avg_italy = avg_italy.loc[avg_italy['prezzo'] <= max_price]
        self.plot_time_series(avg_italy, period, 'AVERAGE PRICE IN ITALY')

    def sidebar_select_regions(self):
        with st.sidebar:
            REGIONS_SELECTED = st.multiselect('Select regions', self.REGIONI, default=["Lombardia"])
        return REGIONS_SELECTED

    def plot_average_by_region(self, df, period, regions, max_price=5000):
        avg_by_region = self.get_week_and_month(df)
        avg_by_region = avg_by_region.loc[df['regione'].str.title().isin(regions)]
        avg_by_region = avg_by_region.groupby([period, 'regione'])['prezzo'].mean().reset_index()
        avg_by_region = avg_by_region.loc[avg_by_region['prezzo'] < max_price]
        self.plot_time_series(avg_by_region, period, 'AVERAGE PRICE BY REGION', 'regione')

    def sidebar_select_municipalities(self):
        df = self.load_data()
        with st.sidebar:
            MUNICIPALITIES_SELECTED = st.multiselect('Select cities', df['citta'].unique(), default=["Milano"])
        return MUNICIPALITIES_SELECTED

    def plot_average_by_municipality(self, df, period, municipalities, max_price=5000):
        avg_by_municipality = self.get_week_and_month(df)
        avg_by_municipality = avg_by_municipality.loc[df['citta'].str.title().isin(municipalities)]
        avg_by_municipality = avg_by_municipality.groupby([period, 'citta'])['prezzo'].mean().reset_index()
        avg_by_municipality = avg_by_municipality.loc[avg_by_municipality['prezzo'] < max_price]
        self.plot_time_series(avg_by_municipality, period, 'AVERAGE PRICE BY CITY', 'citta')

    def sidebar_select_city(self):
        df = self.load_data()
        with st.sidebar:
            CITY = st.selectbox('SELECT ONE CITY', df['citta'].unique())
            st.write('You selected:', CITY)
        return CITY

    def sidebar_select_neighbourhoods(self, municipality):
        df = self.load_data()
        df = df.loc[df['citta'].str.title() == municipality][['prezzo', 'quartiere', 'citta', 'regione']]
        freq = df['quartiere'].value_counts().sort_values(ascending=False)[0:5]
        with st.sidebar:
            NEIGHBOURHOODS_SELECTED = st.multiselect('Select neighbourhoods', list(df['quartiere'].unique()), default=list(freq.index))
        return NEIGHBOURHOODS_SELECTED

    def plot_average_by_neighbourhoods(self, df, period, city, neighbourhoods, max_price=5000):
        avg_by_neighborhood = self.get_week_and_month(df)
        avg_by_neighborhood = avg_by_neighborhood.loc[df['citta'].str.title() == city]
        avg_by_neighborhood = avg_by_neighborhood.loc[df['quartiere'].str.title().isin(neighbourhoods)]
        avg_by_neighborhood = avg_by_neighborhood.groupby([period, 'quartiere'])['prezzo'].mean().reset_index()
        avg_by_neighborhood = avg_by_neighborhood.loc[avg_by_neighborhood['prezzo'] < max_price]
        self.plot_time_series(avg_by_neighborhood, period, 'AVERAGE PRICE BY NEIGHBOURHOODS', 'quartiere')

    def main(self, period, regions, municipalities, city, neighbourhoods, max_price=5000, time_start="01-01-2023", time_end=TODAY):
        df = self.load_data()
        df = self.clean_data(df, time_start, time_end)

        #st.write("#### **AVERAGE PRICE IN ITALY")
        self.plot_average_italy(df, period, max_price)
        st.write('-'*20)

        #st.write("#### **AVERAGE PRICE PER REGION")
        self.plot_average_by_region(df, period, regions, max_price)
        st.write('-'*20)

        #st.write("#### **AVERAGE PRICE IN MUNICIPALITY")
        self.plot_average_by_municipality(df, period, municipalities, max_price)
        st.write('-'*20)

        #st.write("#### **AVERAGE PRICE PER NEIGHBORHOOD")
        self.plot_average_by_neighbourhoods(df, period, city, neighbourhoods, max_price)







