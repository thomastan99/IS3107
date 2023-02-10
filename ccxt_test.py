api_key =  "e70b1b8c-4edb-4628-a737-2d054da37c12"
# top 20 coins by market cap 
#https://docs.coincap.io/
import requests
import pandas as pd
from helper_functions import flatten_nested_json_df

url = "http://api.coincap.io/v2/assets"

payload={}
headers = {"limit":"2000"}
#Gathering the data from the API
response = requests.request("GET", url, headers=headers, data=payload)
results = pd.read_json(response.text)
results = flatten_nested_json_df(results)
#Cleaning the data to ensure that the marketCap column is float
#to-do clean the other data rows also
results = results.astype({"data.marketCapUsd": 'float32'})
results = results.sort_values("data.marketCapUsd", ascending= False)
top_10_coins = results.head(20)
print(top_10_coins)
top_10_coins.to_excel("sampledata_assets.xlsx")
success_list = []
error_list = []
for coin in top_10_coins["data.name"]:
    coin = coin.lower()
    print(coin)
    url = f"http://api.coincap.io/v2/assets/{coin}/history?interval=d1"
    # url = "http://api.coincap.io/v2/assets/bitcoin/history?interval=d1"
    print(url)
    payload={}
    headers = {}

    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        results = pd.read_json(response.text)
        results = flatten_nested_json_df(results)
        results.to_excel(f"sampledata_{coin}.xlsx")
        print(results.head())
        print(results.columns)
        success_list.append(coin)

    except:
        print(url)
        error_list.append(coin)
        pass

print(success_list, error_list)


