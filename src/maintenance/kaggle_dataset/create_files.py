#%%
import os
import pandas as pd

names = ["rents_clean", "rent_raw", "sale_clean", "sale_raw"]

for index, file in enumerate(os.listdir("../dataframes")):
    df = pd.read_parquet(f"../dataframes/{file}", engine='fastparquet')
    df.to_csv(f"{names[index]}.csv", index=False)
