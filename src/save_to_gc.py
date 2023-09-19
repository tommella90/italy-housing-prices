#%%
#https://console.cloud.google.com/storage/browser/italy-housing-data;tab=objects?forceOnBucketsSortingFiltering=true&hl=it&project=italy-housing&prefix=&forceOnObjectsSortingFiltering=false

import pandas as pd
import numpy as np
from datetime import date, datetime
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credential_config/application_default_credentials.json"
storage_client = storage.Client(project="italy-housing")
bucket = storage_client.bucket("italy-housing-data")

blob = bucket.blob("rents/rents_raw.parquet")
blob.upload_from_filename("dataframes/rents_raw.parquet")

#blob = bucket.blob("rents/rents_clean.parquet")
#blob.upload_from_filename("dataframes/italy_housing_price_rent_clean.parquet.gzip")

blob = bucket.blob("sales/sales_raw.parquet")
blob.upload_from_filename("dataframes/sales_raw.parquet")

#blob = bucket.blob("sales/sales_clean.parquet")
#blob.upload_from_filename("dataframes/italy_housing_price_sale_clean.parquet.gzip")


# %%
