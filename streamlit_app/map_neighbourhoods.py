import pandas as pd
import numpy as np
from datetime import date, datetime
import plotly.express as px
import streamlit as st


class MapPriceNeighbourhoods:

    TODAY = np.datetime64(date.today())
    FULL_CALENDAR = pd.DataFrame(pd.date_range(start="2023-01-01", end=TODAY), columns=['datetime'])

    def __init__(self,
                 df_path="dataframes/italy_housing_price_rent_clean.parquet.gzip",
                 municipality_coords_path="geodata/municipalities_centroids.csv",
                 region_coords_path="geodata/regions_centroids.csv",
                 provinces="geodata/province-italiane.xlsx"):
        self.df_path = df_path
        self.municipality_coords_path = municipality_coords_path
        self.region_coords_path = region_coords_path
        self.provinces_path = provinces

    #../dataframes/italy_housing_price_rent_raw.parquet.gzip
    def load_data(self):
        df = pd.read_parquet(self.df_path)
        municipality_coords = pd.read_csv(self.municipality_coords_path)
        region_coords = pd.read_csv(self.region_coords_path)
        return df, municipality_coords, region_coords

    def clean_data(self, df):
        # GEOGRAPHIC INGO
        df['regione'] = df['regione'].str.title()
        df['citta'] = df['citta'].str.title()
        df['quartiere'] = df['quartiere'].str.title()

        # OUTLIERS
        high_quantiles = df['prezzo'].quantile(0.98)
        df = df[df['prezzo']<high_quantiles]

        # REMOVE MISSINGS
        df = df.dropna(subset=['regione', 'citta', 'prezzo'])

        return df

    def get_mean_price_by_area(self, df, area, operation):
        return df.groupby([area, "lat", "lon"]).agg({'prezzo': operation}).reset_index().sort_values(by=['prezzo'], ascending=True)
        #return df['prezzo'].groupby(df[area]).mean().sort_values(ascending=True)


    def price_per_neighbourhoods(self, df, city, operation="mean"):
        df = df.loc[df['citta'] == city]

        # city and neighboors cooridnates
        city_jsn = pd.read_json(f"geodata/prov_coords/{city}.json").T
        city_jsn.columns = ['lat', 'lon']
        df = df.merge(city_jsn, left_on='quartiere', right_on=city_jsn.index, how='left')

        # group by neighbourhood
        df_neighbourhoods = self.get_mean_price_by_area(df, 'quartiere', operation)

        # centroids
        centroids = city_jsn.loc[city_jsn.index==city, ['lat', 'lon']]

        return df_neighbourhoods, centroids

    def slice_dataframe(self, df, date_start="2023-01-01", date_end=TODAY, min_price=0, max_price=20000):
        df = df.loc[(df['datetime'] >= date_start) & (df['datetime'] <= date_end)]
        df = df[(df['prezzo'] >= min_price) & (df['prezzo'] <= max_price)]
        return df

    def side_bar_price_range(self):
        df = pd.read_parquet(self.df_path)
        df = self.clean_data(df)
        max_value = int(round(df['prezzo'].max()))
        df = self.slice_dataframe(df, max_price=max_value)
        max_value = int(round(df['prezzo'].max()))
        price_ranges_city = st.slider('SELECT A PRICE RANGE',
                                      key="price_range",
                                      min_value=0, max_value=max_value,
                                      value=(200, 4000))
        st.write('You selected:', price_ranges_city)
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

    def side_bar_city(self):
        provinces = pd.read_excel(self.provinces_path)
        provinces = list(provinces['Provincia'])
        st.sidebar.header("PROVINCIA")
        default_province = 'Milano'
        select_province = st.sidebar.selectbox(
            "Choose a province", provinces,
            index=provinces.index(default_province)
        )
        return select_province


    def map_city(self, df, city, operation="mean"):
        df_neighbourhoods, centroids = self.price_per_neighbourhoods(df, city, operation)

        fig = px.scatter_mapbox(df_neighbourhoods, lat="lat", lon="lon",
                                hover_name="quartiere",
                                hover_data=["prezzo"],
                                color="prezzo",
                                color_continuous_scale="turbo",
                                size="prezzo",
                                zoom=11,
                                center=dict(lat=centroids.iloc[0,0], lon=centroids.iloc[0,1]),
                                height=600, width=800
                                )

        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(margin={"r":5,"t":0,"l":0,"b":0})

        st.plotly_chart(fig)



    # MAIN
    def main(self, date_start, date_end, min_price, max_price, city="Milano", operation="mean"):
        df, coordinates_df, region_coords = self.load_data()
        df = self.slice_dataframe(df, date_start, date_end, min_price, max_price)
        self.map_city(df, city, operation)


#%%
