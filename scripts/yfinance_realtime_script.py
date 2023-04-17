import os
import time

import numpy as np
import yfinance as yf
from datetime import datetime, timedelta  
import datetime as dt
from dateutil import tz
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
        bigquery.SchemaField("Price", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("Volume", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("Close", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("High", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("Low", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("Open", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("Previous_Price", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("Previous_Volume", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("Datetime", "DATETIME", mode="REQUIRED")
    ]

    job_config = bigquery.LoadJobConfig(schema=schema)

    start = True
    prev_item = {}

    while True:

      new_item = yf.download(ticker, interval = "1m", period = "1d").tail(1)
      new_item.reset_index(inplace = True)
      new_item = new_item.assign(Price=new_item['Open'])
      new_item = new_item.drop(columns=['Adj Close'])
    
      if start == True:
        prev_item["Price"] = new_item["Price"]
        prev_item["Volume"] = new_item["Volume"]
        start = False
      else:
        new_item["Previous_Price"] = prev_item["Price"]
        new_item["Previous_Volume"] = prev_item["Volume"]

        prev_item["Price"] = new_item["Price"]
        prev_item["Volume"] = new_item["Volume"]
        time.sleep(delay)
        client.load_table_from_dataframe(new_item, table_id, job_config=job_config)
        time.sleep(delay)
