# HOUSE PRICES IN ITALY
### This repository contains data on house rents and sales announcements in Italy, scraped with Beautiful Soup on [Immobiliare.com](https://www.immobiliare.it/vendita-case/milano/?criterio=rilevanza)

The **housing_modules** folder contains the script which scrapes the data from the web and clean those data. 

The data are then saved in **dataframes**. It contains parquet files of both cleand and raw data, and csv cleaned data.

The **main.py** runs the previous scripts, automatically scraping and cleaning the data. If you run it, you have to specify the option: *how many webpages do you want to scrapre?* (maximum 80)

## How to use
To run the main file and download existing data plus new data: 

1) Clone the repository from git: 
```
git clone https://github.com/tommella90/italy-housing-price/
```

2) install pipenv (check [here](https://realpython.com/pipenv-guide/))
```
pip install pipenv
```

3) Activate the environment
```
pipenv shell
```

4) Run the main file and choose the number of webpages to scrape per city (maximum 80): 
```
pipenv run python main.py
```

## Streamlit app
It also contains the **STREAMLIT APP**, with 4 main functionalities: 
1) **ITALIAN MAP**: shows the average price per municipality on the Italian map 
2) **MUNICIPALITY MAP**: select a municipality, and see the average price per neighbourhoods 
3) **TIME SERIES ANALYSIS**: show the average price in Italy, per region, city and neighbourhoods
4) **AFFORDABILITY**: choose a price range and compare what you can afford in different cities (squared meters, rooms, etc...)

[STREAMLIT LINK HERE](https://tommella90-italy-house-prici-streamlit-appstreamlit-main-p06j3n.streamlit.app/)
____________________________________