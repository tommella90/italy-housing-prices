#import requests
#response = requests.get('https://httpbin.org/ip')
#print('Your IP is {0}'.format(response.json()['origin']))
import subprocess
import credential_config.get_credentials as gc


# get user and create dataframes folder
user_name, user_email = gc.get_git_user_info()
if user_name == "Tommaso Ramella" and user_email == "tommaso.ramella90@gmail.com":
    pass
else: 
    #subprocess.check_output(['mkdir', 'dataframes'])
    gc.query_housing_data("rents/rents_raw.parquet", "dataframes/rents_raw.parquet")
    gc.query_housing_data("rents/rents_clean.parquet", "dataframes/rents_clean.parquet")
    gc.query_housing_data("sales/sales_raw.parquet", "dataframes/sales_raw.parquet")
    gc.query_housing_data("sales/sales_clean.parquet", "dataframes/sales_clean.parquet")

    
    