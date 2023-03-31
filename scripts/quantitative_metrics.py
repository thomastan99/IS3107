import os
import pandas as pd

from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"
client = bigquery.Client()
def pull_coin_data(coin_name):

    query = f"""
    with deduped_table as (
    select distinct * from `crypto3107.binance_data_new.{coin_name}_combined`
    )
    SELECT *
    from deduped_table

    order by Date desc

    """

    results = client.query(query).to_dataframe()
    results.Date = pd.to_datetime(results.Date)
    return results

final_df = pull_coin_data('bitcoin')
final_df.to_csv(f"quantitative_data.csv")

