import os

from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"
client = bigquery.Client()
def pull_coin_data(coin_name):

    query = f"""
    with deduped_table as (
    select distinct * from `crypto3107.binance_data_new.{coin_name}`
    )
    SELECT 
    date, open, high, low, close, volume,
    priceUsd,
    CASE 
        WHEN priceUsd > LAG(priceUsd) OVER (ORDER BY date) THEN 1 
        ELSE 0 
    END AS is_greater
    from deduped_table

    order by date desc

    """

    results = client.query(query).to_dataframe()

    results.to_csv(f"assets/{coin_name}_ml_data.csv")


