import pandas as pd
import numpy as np
from datetime import date, datetime
import matplotlib.pyplot as plt
import plotly.express as px
import streamlit as st


class MapPriceItaly:

    TODAY = np.datetime64(date.today())
    FULL_CALENDAR = pd.DataFrame(pd.date_range(start="2023-01-01", end=TODAY), columns=['datetime'])

    def __init__(self,
                 df_path="dataframes/italy_housing_price_rent_clean.parquet.gzip",
                 municipality_coords_path="geodata/municipalities_centroids.csv",
                 region_coords_path="geodata/regions_centroids.csv"):
        self.df_path = df_path
        self.municipality_coords_path = municipality_coords_path
        self.region_coords_path = region_coords_path

    #../dataframes/italy_housing_price_rent_raw.parquet.gzip
    def load_data(self):
        df = pd.read_parquet(self.df_path)
        municipality_coords = pd.read_csv(self.municipality_coords_path)
        region_coords = pd.read_csv(self.region_coords_path)
        return df, municipality_coords, region_coords

    def select_time_range(self, df, time_start, time_end):
        df = df.loc[(df['datetime'] >= time_start) & (df['datetime'] <= time_end)]
        return df

    def clean_data(self, df):
        df['regione'] = df['regione'].str.title()
        df['citta'] = df['citta'].str.title()

        # OUTLIERS
        high_quantiles = df['prezzo'].quantile(0.98)
        df = df[df['prezzo']<high_quantiles]

        # REMOVE MISSINGS
        df = df.dropna(subset=['regione', 'citta', 'prezzo'])

        return df

    def get_mean_price_by_area(self, df, area, operation):
        return df.groupby([area, "lat", "lon"]).agg({'prezzo': operation}).reset_index().sort_values(by=['prezzo'], ascending=True)
        #return df['prezzo'].groupby(df[area]).mean().sort_values(ascending=True)

    def price_per_region(self, df):
        prices_by_region = self.get_mean_price_by_area(df, 'regione', operation="mean")
        fig1, ax = plt.subplots()
        ax.barh(prices_by_region.index, prices_by_region)
        ax.set_title("ITALIAN RENTS:\n Price by region")
        ax.set_xlabel("Euros")
        ax.set_ylabel("Region")
        st.pyplot(fig1)

    def map_price_by_region(self, df, geo_data):
        prices_by_region = self.get_mean_price_by_area(df, 'regione', operation="mean")
        df_price = pd.DataFrame(prices_by_region)
        df_price = df_price.reset_index()
        df_price.columns = ['reg_name', 'prezzo']
        geo_data_region = geo_data.merge(df_price, on = 'reg_name')
        geo_data_region['prezzo'] = round(geo_data_region['prezzo'].astype(float), 2)

    def barplot_price_per_municipality(self, df, EXTREME_CASES=10):
        prices_by_municipality = self.get_mean_price_by_area(df, 'citta', operation="mean")
        prices_by_municipality = prices_by_municipality.sort_values(ascending=True)
        prices_by_municipality_extremes = pd.concat(
            [prices_by_municipality.head(EXTREME_CASES),
             prices_by_municipality.tail(EXTREME_CASES)],
            axis=0)

        fig1, ax = plt.subplots()
        ax.barh(prices_by_municipality_extremes.index, prices_by_municipality_extremes)
        ax.set_title("ITALIAN RENTS:\n Price by city")
        ax.set_xlabel("Euros")
        ax.set_ylabel("Municipality")
        st.pyplot(fig1)

    def slice_dataframe(self, df, date_start="2023-01-01", date_end=TODAY, min_price=0, max_price=20000):
        df = df.loc[(df['datetime'] >= date_start) & (df['datetime'] <= date_end)]
        df = df[(df['prezzo'] >= min_price) & (df['prezzo'] <= max_price)]
        return df

    def map_municipalities(self, df, operation, coordinates_df, date_start, date_end, min_price, max_price):

        # slice dataframe
        df = self.slice_dataframe(df, date_start, date_end, min_price, max_price)

        # group by municipality
        prices_by_municipality = df.merge(coordinates_df, left_on='citta', right_on='name', how='left')
        prices_by_municipality = prices_by_municipality.groupby(["citta", "lat", "lon", "regione"]).agg({'prezzo': operation}).reset_index().sort_values(by=['prezzo'], ascending=True)
        prices_by_municipality = prices_by_municipality[['citta', 'lat', 'lon', 'prezzo', 'regione']]
        prices_by_municipality = prices_by_municipality.dropna(subset=['lat', 'lon'])

        # plot
        fig = px.scatter_mapbox(
            prices_by_municipality, lat="lat", lon="lon",
            hover_name="citta",
            hover_data=["prezzo"],
            color="prezzo",
            color_continuous_scale="turbo",
            range_color=(min_price, max_price),
            size="prezzo",
            zoom=5,
            center=dict(lat=41.8719, lon=12.5674),
            opacity=0.5,
            labels={'prezzo': 'Price in €'},
            height=800, width=800
                                )
        fig.update_layout(mapbox_style="open-street-map")

        fig.update_layout(margin={"r":5,"t":0,"l":0,"b":0})
        st.plotly_chart(fig)


    def side_bar_price_range(self):
        df = pd.read_parquet(self.df_path)
        df = self.clean_data(df)
        max_value = int(round(df['prezzo'].max()))
        df = self.slice_dataframe(df, max_price=max_value)
        max_value, min_value = int(round(df['prezzo'].max())), int(round(df['prezzo'].min()))
        price_ranges_city = st.slider('SELECT A PRICE RANGE',
                                      key="price_range",
                                      min_value=min_value, max_value=max_value,
                                      value=(400, 1000))
        #st.write('You selected:', price_ranges_city)
        return price_ranges_city[0], price_ranges_city[1]

    def box_choice_math_operation(self):
        math_option = st.sidebar.selectbox(
            "WHAT DO YOU WANT TO SEE? 👉",
            key="wathever",
            options=["mean", "median", "max"]
        )
        return math_option

    def side_bar_time_range(self):
        today_string = np.datetime_as_string(self.TODAY, unit='D')
        start_time = datetime.strptime("2023-01-01", "%Y-%m-%d")
        end_time = datetime.strptime(today_string, "%Y-%m-%d")
        date_values = st.slider('SELECT A DATE RANGE',
                                min_value=start_time,
                                max_value=end_time,
                                value=(start_time, end_time),
                                format="YYYY-MM-DD")
        return date_values[0], date_values[1]

    # MAIN
    def main(self, operation, date_start, date_end, min_price, max_price):
        df, coordinates_df, region_coords = self.load_data()
        df = self.select_time_range(df, date_start, date_end)
        df = self.clean_data(df)

        #df = self.clean_data(df)
        st.markdown("## AVERAGE PRICE BY MUNICIPALITY")
        self.map_municipalities(df, operation, coordinates_df, date_start, date_end, min_price, max_price)

#%%
