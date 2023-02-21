api_key =  "e70b1b8c-4edb-4628-a737-2d054da37c12"
# top 20 coins by market cap 
#https://docs.coincap.io/
import os
from io import StringIO

import pandas as pd
import requests

from scripts.helper_functions import flatten_nested_json_df, push_to_GBQ


def extract_coincap_api(dag_path):
    url = "http://api.coincap.io/v2/assets"
    payload={}
    headers = {"limit":"2000"}
    
    #Gathering the data from the API
    response = requests.request("GET", url, headers=headers, data=payload)
    results = pd.read_json(StringIO(response.text))
    results = flatten_nested_json_df(results)
    
    #Cleaning the data to ensure that the marketCap column is float
    #to-do clean the other data rows also
    results = results.astype({"data.marketCapUsd": 'float32'})
    results = results.sort_values("data.marketCapUsd", ascending= False)
    top_10_coins = results.head(10)
    top_10_coins = top_10_coins.drop(["index", "data.maxSupply", "data.explorer"], axis = 1)
    top_10_coins = top_10_coins[["data.id", "data.supply"]]
    top_10_coins=  top_10_coins.rename(columns = {'data.id':'data_id', 'data.supply': "data_supply"})
    print(top_10_coins)
    top_10_coins.to_csv("sampledata_assets.csv", index = False)
    push_to_GBQ("sampledata_assets.csv", "IS3107.main")
    success_list = []
    error_list = []
    
    for coin in top_10_coins["data_id"]:
        coin = coin.lower()
        print(coin)
        url = f"http://api.coincap.io/v2/assets/{coin}/history?interval=d1"
        print(url)
        payload={}
        headers = {}


        response = requests.request("GET", url, headers=headers, data=payload)
        results = pd.read_json(response.text)
        # results = flatten_nested_json_df(results)
        # results = results.drop(['timestamp', 'data.time', 'index'], axis= 1)
        # results["data.date"] = pd.to_datetime(results['data.date']).dt.date
        results.to_csv(f"sampledata_{coin}.csv", index = False)
        push_to_GBQ(f"sampledata_{coin}.csv", f"IS3107.{coin}")
        print(results.head())
        success_list.append(coin)

    print(f"success list : {success_list}, error list : {error_list}")


