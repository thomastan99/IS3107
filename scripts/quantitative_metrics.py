from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"
client = bigquery.Client()

query = """
with deduped_table as (
  select distinct * from `crypto3107.coincap.bitcoin`
)
SELECT 
DATE(PARSE_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', date)) AS date,
  priceUsd,
  CASE 
    WHEN priceUsd > LAG(priceUsd) OVER (ORDER BY date) THEN 1 
    ELSE 0 
  END AS is_greater
from deduped_table

order by date desc

"""

results = client.query(query).to_dataframe()

print(results)
