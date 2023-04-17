import os
import time

import numpy as np
import yfinance as yf
from google.cloud import bigquery

########################## ALL CREDENTIALS - BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

def extract_yfinance_realtime_data(ticker, delay=60):  
    print(f"The following is the data for {ticker} every {delay} seconds delay")
    count = 0
    
    if ticker == 'ETH-USD':
        coin = 'ethereum'
    elif ticker == 'BTC-USD':
        coin = 'bitcoin'
    elif ticker == 'XRP-USD':
        coin = 'xrp'
    
    client = bigquery.Client()
    table_id = f'crypto3107.streaming.{coin}'
    prev_item = None
    
    schema = [
        bigquery.SchemaField("Price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("Volume", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("Previous_Price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("Previous_Volume", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("Datetime", "DATETIME", mode="NULLABLE"),
        
    ]


    job_config = bigquery.LoadJobConfig(schema=schema)

    while True:
      if count != 0:
        time.sleep(delay)
      new_item = yf.download(ticker, interval = "1m", period = "1d").tail(1)
      new_item = new_item.rename(columns={'Open': 'Price'})
      new_item = new_item.drop(columns=['High', 'Low', 'Close', 'Adj Close'])
      if not prev_item is None:
        new_item["Previous_Price"] = prev_item["Price"][0]
        new_item["Previous_Volume"] = prev_item["Volume"][0]
      else:
        new_item["Previous_Price"] = np.nan
        new_item["Previous_Volume"] = np.nan
      
      if (new_item["Previous_Price"][0] != np.nan and new_item["Previous_Volume"][0] != np.nan):
        client.load_table_from_dataframe(
          new_item, table_id, job_config=job_config
        )
      prev_item = new_item
      
      count += 1
      
extract_yfinance_realtime_data("BTC-USD")