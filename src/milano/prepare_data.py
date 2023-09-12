#%%
import pandas as pd
df = pd.read_parquet("milano_sales.parquet")
df["Riferimento e Data annuncio"]
#%%
parameters = {
       "citta": citta,
       "stanze": [3, 4],
       "superficie": [30, 100],
       "prezzo": [400000],
       #"Riferimento e Data annuncio"
}
