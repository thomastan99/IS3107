from google.cloud import bigquery
from google.oauth2 import service_account


def insert_data_into_BQ(coin_name):
    schema = [
        bigquery.SchemaField("priceUsd", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date", "STRING", mode="NULLABLE"),
    ]

    client = bigquery.Client(location="asia-southeast1")
    table_id = f"crypto3107.coincap.{coin_name}"
    table_exists = False
    tables = client.list_tables('coincap')
    for table in tables:
        if table.table_id == coin_name:
            table_exists = True
            break

    # If the table does not exist, create it
    if not table_exists:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # API request
        print(f'Table {table.table_id} was created.')
    
    with open(f'./assets/coincap_{coin_name}_data.json', 'rb') as f:
        job_config = bigquery.LoadJobConfig(
            source_format='NEWLINE_DELIMITED_JSON',
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()



def push_to_gbq(coin):
    import datetime

    import pandas as pd
    import yfinance as yf

    start_date = datetime.date.today() - datetime.timedelta(days=3*365)
    end_date = datetime.date.today()

    def get_data(coin):
        data = yf.download(coin, start=start_date, end=end_date, interval="1d")
        data = data.rename(columns= {'Adj Close': "Price"})
        data = data.reset_index(drop=False).rename(columns={'index': 'Date'})
        data = data.reset_index(drop=True)
        return data
    ticker = ""
    if coin == "ethereum":
        ticker = 'ETH-USD'
    elif coin == "bitcoin":
        ticker = 'BTC-USD'
    elif coin == "xrp":
        ticker = 'XRP-USD'
    df = get_data(ticker)
    table_id = f'crypto3107.binance_data_new.{coin}'
    ##Hardcoded Creds Here need to be fixed 
    creds = service_account.Credentials.from_service_account_file('./creds/cred.json')
    df.to_gbq(table_id, location="asia-southeast1", reauth=False, project_id='crypto3107', if_exists='replace', credentials=creds)