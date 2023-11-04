
#%%
import pandas as pd
import numpy as np
from datetime import date
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio
import streamlit as st
#from streamlit_folium import st_folium
from datetime import datetime
from maps_italy import MapPriceItaly

## CONFIG ##
st.set_page_config(layout="wide",
                   initial_sidebar_state="expanded",
                   page_title="spotify_recommender",
                   page_icon=":🧊:")


m = st.markdown("""
<style>
section[data-testid="stSidebar"] .css-ng1t4o {{width: 10rem;}}

div.stButton > button:first-child {
    background-color: #141c7a;
    color: white;
    height: 3em;
    width: 5em;
    border-radius:10px;
    border:3px solid #000000;
    font-size:30px;
    font-weight: bold;
    margin: auto;
    display: block;
}


div.stButton > button:hover {
	background:linear-gradient(to bottom, #ce1126 5%, #ff5a5a 100%);
	background-color:#ce1126;
}

div.stButton > button:active {
	position:relative;
	top:3px;
	
}

</style>""", unsafe_allow_html=True)

import streamlit as st

TODAY = np.datetime64(date.today())

FULL_CALENDAR = pd.DataFrame(pd.date_range(start="2023-01-01", end=TODAY), columns=['datetime'])

REGIONI = ('ITALY',
           'Abruzzo', 'Basilicata', 'Calabria', 'Campania', 'Emilia-Romagna',
           'Friuli-Venezia Giulia', 'Lazio', 'Liguria', 'Lombardia', 'Marche',
           'Molise', 'Piemonte', 'Puglia', 'Sardegna', 'Sicilia', 'Toscana',
           'Veneto', 'Valle-D-Aosta', 'Trentino-Alto-Adige'
           )



#%% FUNCTIONS
st.title("ITALIAN RENTS")

st.markdown("### This app show the average price of rents in Italy")
st.markdown("###**Data source:** [immobiliare.it](https://www.immobiliare.it/)")

st.write("-"*10)
st.markdown("##What are you interested in?")
columns = st.columns(4)
col1, col2, col3 , col4, col5 = st.columns(5)
col_titles = ['Maps', 'Time-series', 'affordability', 'other']

selection = 0
for index, col in enumerate(columns):
    with col:
        st.markdown(col_titles[index])
        if st.button(f"{index+1}", key=f"{index+1}"):
            selection = int(index)

st.write(selection)


st.write("-"*10)

# import os 
# st.write(os.listdir())

#%%

## FUNCTIONALITIES
@st.cache_data
def load_data():
    df = pd.read_parquet("dataframes/italy_housing_price_rent_raw.parquet.gzip")
    municipality_coords = pd.read_csv("geodata/municipalities_centroids.csv")
    region_coords = pd.read_csv("geodata/regions_centroids.csv")
    # dd
    return df, municipality_coords, region_coords


def clean_data(df):

    # PRICE
    df['prezzo'] = df['prezzo'].str.replace('€', '')
    df['prezzo'] = df['prezzo'].str.replace('[^0-9.]', '', regex=True)
    df['prezzo'] = df['prezzo'].str.replace('.', '')
    df['prezzo'][df['prezzo'] == ''] = np.nan
    df['prezzo'] = df['prezzo'].astype(float)

    # GEOGRAPHIC INFO
    df['regione'] = df['regione'].str.title()
    df['citta'] = df['citta'].str.title()

    # DATETIME INFO
    df = df.rename(columns={'Riferimento e Data annuncio': "data"})
    date_regex = r'(\d{2}/\d{2}/\d{4})'
    df['datetime'] = df['data'].str.extract(date_regex)
    df['datetime'] = pd.to_datetime(df['datetime'], format='%d/%m/%Y')
    df = df.loc[(df['datetime'] > '2023-01-01') & (df['datetime'] < TODAY)]
    df = pd.merge(df, FULL_CALENDAR, how='outer', on='datetime')

    df = df.dropna(subset=['regione', 'citta', 'prezzo'])
    return df

def get_mean_price_by_area(df, area):
    return df['prezzo'].groupby(df[area]).mean().sort_values(ascending=True)

def price_per_region(df):
    prices_by_region = get_mean_price_by_area(df, 'regione')
    fig1, ax = plt.subplots()
    ax.barh(prices_by_region.index, prices_by_region, )
    ax.set_title("ITALIAN RENTS:\n Price by region")
    ax.set_xlabel("Euros")
    ax.set_ylabel("Region")
    st.pyplot(fig1)

def map_by_region(df, geo_data):
    prices_by_region = get_mean_price_by_area(df, 'regione')
    df_price = pd.DataFrame(prices_by_region)
    df_price = df_price.reset_index()
    df_price.columns = ['reg_name', 'prezzo']
    geo_data_region = geo_data.merge(df_price, on = 'reg_name')
    geo_data_region['prezzo'] = round(geo_data_region['prezzo'].astype(float), 2)

    map = folium.Map(location=[41.8719, 12.5674], zoom_start=5)

    folium.Choropleth(
        geo_data=geo_data_region,
        data=geo_data_region,
        columns=['reg_name', 'prezzo'],
        key_on='feature.properties.reg_name',
        fill_color='YlGn',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name='Your Choropleth Title'
    ).add_to(map)

    highlights = folium.features.GeoJson(
        geo_data_region,
        style_function= lambda x:
        {"color": "red",
         #"fillColor":"YlOrBr",
         "weight":1,
         "fillOpacity":0,
         },
        highlight_function = lambda x: {
            'fillColor': '#000000',
            'color': '#000000',
            'fillOpacity': 0.50,
            'weight': 0.5
        },
        tooltip=folium.features.GeoJsonTooltip(
            fields=['reg_name' , 'prezzo'],
            aliases = [ 'REGION:' , 'Mean price in euro:' ],
            labels = True,
            sticky = False)
    )

    map.add_child(highlights)
    map.keep_in_front(highlights)
    folium.LayerControl().add_to(map)

    st_data = st_folium(map, width=725)
    return st_data

def price_per_municipality(df, geo_data, EXTREME_CASES=10):
    prices_by_municipality = get_mean_price_by_area(df, 'citta')
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

def map_by_municipality(df, geo_data):
    prices_by_region = get_mean_price_by_area(df, 'citta')
    df_price = pd.DataFrame(prices_by_region)
    df_price = df_price.reset_index()
    df_price.columns = ['name', 'prezzo']
    geo_data_municipality = geo_data.merge(df_price, on = 'name')
    geo_data_municipality['prezzo'] = round(geo_data_municipality['prezzo'].astype(float), 2)

    map = folium.Map(location=[41.8719, 12.5674], zoom_start=4.8)

    folium.Choropleth(
        geo_data=geo_data_municipality,
        data=geo_data_municipality,
        columns=['name', 'prezzo'],
        key_on='feature.properties.name',
        fill_color='YlGn',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name='Price by municipality'
    ).add_to(map)

    folium.LayerControl().add_to(map)

    st_folium(map, width=725)


def map_municipalities(df, coordinates_df, date_start, date_end, min_price, max_price):

    # slice dataframe
    df = df.loc[(df['datetime'] >= date_start) & (df['datetime'] <= date_end)]
    df = df[(df['prezzo'] >= min_price) & (df['prezzo'] <= max_price)]

    # group by municipality and region
    df = df.groupby(['citta', 'regione'])['prezzo'].mean().reset_index()
    df = df.merge(coordinates_df, left_on='citta', right_on='name', how='left')
    df = df[['citta', 'lat', 'lon', 'prezzo', 'regione']]
    df = df.dropna(subset=['lat', 'lon'])


    fig = px.scatter_mapbox(df, lat="lat", lon="lon",
                            hover_name="citta",
                            hover_data=["prezzo"],
                            color="prezzo",
                            color_continuous_scale="turbo",
                            range_color=(0, 1000),
                            size="prezzo",
                            zoom=4.3,
                            center=dict(lat=41.8719, lon=12.5674)
                            )

    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":5,"t":0,"l":0,"b":0})

    st.plotly_chart(fig)



#%% MAPS
italy_mapper = MapPriceItaly()



df, municipalities_centroids, regions_centroids = load_data()
df = clean_data(df)

price_ranges_city = st.slider('Select a price range',
                         min_value=0, max_value=20000,
                         value=(0, 5000))
min_price = price_ranges_city[0]
max_price = price_ranges_city[1]


st.checkbox("Whant do you want to see?", key="disabled")
math_option = st.radio(
    "Select: 👉",
    key="visibility",
    options=["mean", "median", "max"]
)

def price_per_neighbourhoods(df, city, price_limit=5000):
    df = df.loc[df['citta'] == city]

    # city and neighboors cooridnates
    city_jsn = pd.read_json(f"../data/prov_coords/{city}.json").T
    city_jsn.columns = ['lat', 'lon']
    df = df.merge(city_jsn, left_on='quartiere', right_on=city_jsn.index, how='left')

    #slice dataframe
    df = df.loc[(df['lat']>40) & (df['lat']<50)]
    df = df.loc[(df['lon']>5) & (df['lon']<15)]
    df = df.loc[df['prezzo']<price_limit]

    # group by neighbourhood
    centroids = city_jsn.loc[city_jsn.index==city, ['lat', 'lon']]
    df_neighbourhoods = df.groupby(['quartiere', 'lat', 'lon'])['prezzo'].agg(math_option).reset_index()
    return df_neighbourhoods, centroids


def map_city(df, city, price_limit=5000):
    df_neighbourhoods, centroids = price_per_neighbourhoods(df, city, price_limit)
    st.write(centroids.iloc[0,0])
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


# bar chart with price by municipality`
def plot_bar_neighbourhoods(df, city, max_price=5000):
    df_neighbourhoods, centroids = price_per_neighbourhoods(df, city, max_price)
    df_neighbourhoods = df_neighbourhoods.sort_values(by='prezzo', ascending=True)
    fig = px.bar(df_neighbourhoods, x='prezzo', y='quartiere',
                 hover_data=['quartiere'], color='prezzo',
                 labels={'Price':'Euros'}, height=2000)
    st.plotly_chart(fig)

provinces = pd.read_excel('geodata/province-italiane.xlsx')
provinces = list(provinces['Provincia'])

st.sidebar.header("PROVINCIA")
default_province = 'Milano'
select_province = st.sidebar.selectbox(
    "Choose a province", provinces,
    index=provinces.index(default_province)
)

#map_city(df, select_province, max_price)
#plot_bar_neighbourhoods(df, select_province, max_price)



#%%

price_values = st.slider('Select a price range',
                         min_value=0, max_value=10000,
                         value=(0, 5000))
min_price = price_values[0]
max_price = price_values[1]

today_string = np.datetime_as_string(TODAY, unit='D')
start_time = datetime.strptime("2023-01-01", "%Y-%m-%d")
end_time = datetime.strptime(today_string, "%Y-%m-%d")

date_values = st.slider('Select a date range',
                        min_value=start_time,
                        max_value=end_time,
                        value=(start_time, end_time),
                        format="YYYY-MM-DD")


map_municipalities(df, municipalities_centroids,
                   date_start=date_values[0], date_end=date_values[1],
                    min_price=min_price, max_price=max_price
                   )



#%% TIME SERIES ANALYSIS
import plotly.graph_objects as go

period = st.selectbox('Select a period', ['day', 'week', 'month'])
if period == 'day':
    period = 'datetime'

def get_week_and_month(df):
    df['week'] = df['datetime'].dt.to_period('W').dt.start_time
    df['month'] = df['datetime'].dt.to_period('M').dt.start_time
    return df

price_limit = st.slider('Select a maximum price',
                        min_value=0, max_value=20000,
                        value=(0, 5000))
LIMIT = price_limit[1]

def plot_time_series(df, period, title, area=None):
    fig = px.line(df, x=period, y='prezzo',
                  color=area,
                  hover_data=[area],
                  title=title,
                  color_discrete_sequence=px.colors.qualitative.Pastel,
                  template="plotly_white",
                  width=800, height=500,
                  )
    fig.update_layout(xaxis_title="Date", yaxis_title="Price (euros)")
    st.plotly_chart(fig)


## italian prices
avg_italy = get_week_and_month(df)
avg_italy = avg_italy.groupby([period])['prezzo'].mean().reset_index()
avg_italy = avg_italy.loc[avg_italy['prezzo'] <= LIMIT]

#plot_time_series(avg_italy, period, 'AVERAGE PRICE IN ITALY')


## prices by region
with st.sidebar:
    REGIONS_SELECTED = st.multiselect('Select regions', df['regione'].unique())

avg_by_region = get_week_and_month(df)
avg_by_region = avg_by_region.loc[df['regione'].isin(REGIONS_SELECTED)]
avg_by_region = avg_by_region.groupby([period, 'regione'])['prezzo'].mean().reset_index()
avg_by_region = avg_by_region.loc[avg_by_region['prezzo'] < LIMIT]

#plot_time_series(avg_by_region, period, 'AVERAGE PRICE BY REGION', 'regione')


# price
with st.sidebar:
    MUNICIPALITIES_SELECTED = st.multiselect('Select cities', df['citta'].unique())

avg_by_municipality = get_week_and_month(df)
avg_by_municipality = avg_by_municipality.loc[df['citta'].isin(MUNICIPALITIES_SELECTED)]
avg_by_municipality = avg_by_municipality.groupby([period, 'citta'])['prezzo'].mean().reset_index()
avg_by_municipality = avg_by_municipality.loc[avg_by_municipality['prezzo'] < LIMIT]

plot_time_series(avg_by_municipality, period, 'AVERAGE PRICE BY CITY', 'citta')



#%%
#df = load_data()
df = pd.read_parquet("dataframes/italy_housing_price_rent_raw.parquet.gzip")
df = clean_data(df)


#%%
MUNICIPALITIES_SELECTED = ['Genova', 'Milano']
price_ranges = st.slider('Select a price range',
                         min_value=0, max_value=100000,
                         value=(0, 5000))
min_price = price_ranges[0]
max_price = price_ranges[1]

df_slice = df.loc[(df['prezzo'] >= min_price) & (df['prezzo'] <= max_price)]
df_slice = df_slice.loc[df_slice['citta'].isin(MUNICIPALITIES_SELECTED)]


#%%

# df_slice['rooms'] = df_slice['stanze'].astype(int)
# #%%
# rooms = df_slice['stanze'].mean()
# st.write('Average number of rooms: ', rooms)


#%%
