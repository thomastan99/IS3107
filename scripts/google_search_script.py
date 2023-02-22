import io
import json
import os
from io import StringIO

import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from pytrends.request import TrendReq

pytrend = TrendReq()

########################## ALL CREDENTIALS - BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

# top 10 crypto coins search terms
coins_dict_1 = ['bitcoin', 'ethereum', 'tether', 'xrp', 'binance']
              
coins_dict_2 = ['cardano', 'polygon', 'dogecoin', 'solana', 'polkadot']

# top 5 crypto news search terms
news_dict = {
    'nft',
    'cryptocurrency news',
    'buy cryptocurrency',
    'best cryptocurrency',
    'crypto exchange'
}


########################## METHOD TO EXTRACT DATA FROM Google ##############################

def get_search_metrics(query_dict): 
        
    pytrend.build_payload(kw_list=query_dict)

    #Referenced code: https://lazarinastoy.com/the-ultimate-guide-to-pytrends-google-trends-api-with-python/ 

    # Related Queries, returns a dictionary of dataframes
    related_queries = pytrend.related_queries()
    related_queries.values()

    #build lists dataframes
    top = list(related_queries.values())[0]['top']
    rising = list(related_queries.values())[0]['rising']

    #convert lists to dataframes

    dftop = pd.DataFrame(top)
    dfrising = pd.DataFrame(rising)

    #join two data frames
    joindfs = [dftop, dfrising]
    all_queries = pd.concat(joindfs, axis=1)

    #function to change duplicates
    cols=pd.Series(all_queries.columns)
    for dup in all_queries.columns[all_queries.columns.duplicated(keep=False)]: 
        cols[all_queries.columns.get_loc(dup)] = ([dup + '.' + str(d_idx) 
                                        if d_idx != 0 
                                        else dup 
                                        for d_idx in range(all_queries.columns.get_loc(dup).sum())]
                                        )
    all_queries.columns=cols

    #rename to proper names
    all_queries.rename({'query': 'top_query', 'value': 'top_query_value', 'query.1': 'related_query', 'value.1': 'related_query_value'}, axis=1, inplace=True) 

    #check your dataset
    all_queries.head(50)
    
    to_convert = []
    for item in range(len(all_queries["top_query"])):
        query_item = {
            'top_query': all_queries['top_query'][item], 
            'query_details': [
                {
                    'top_query_value': int(all_queries['top_query_value'][item]),
                    'related_query': all_queries['related_query'][item],
                    'related_query_value': int(all_queries['related_query_value'][item])
                }
            ]
        }
        to_convert.append(query_item)
        
    #save to json
    queries_json = json.dumps(to_convert)
    return queries_json


########################## METHOD TO UPLOAD DATA TO BQ ##############################

def insert_data_into_BQ(data_as_file) :
    schema = [
        bigquery.SchemaField("top_query", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "query_details",
            "RECORD",
            mode = "REPEATED",
            fields=[
                bigquery.SchemaField("top_query_value", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("related_query", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("related_query_value", "INT64", mode="NULLABLE"),
            ],
        )
    ]

    client = bigquery.Client()
    table_id = "crypto3107.google.batch_queries"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    load_job = client.load_table_from_file(
        data_as_file,
        table_id,
        location="asia-southeast1",
        job_config=job_config,
    ) 

    try:
        load_job.result()
        print("Successfully uploaded data to BigQuery!")
    except BadRequest:
        for error in load_job.errors:
            print(error["message"])
    
    destination_table = client.get_table(table_id)


########################## EXTRACT AND UPLOAD DATA ##############################

# 1. Extract and upload data of 10 coins googles to BQ
def extract_google_coin_data_into_BQ(query_dict):
    coins_google_json = get_search_metrics(query_dict)
    coins_in_json = StringIO(coins_google_json) # Format json to ndjson
    coins_result = [json.dumps(record) for record in json.load(coins_in_json)] 
    coins_formatted_json = '\n'.join(coins_result)
    coins_data_as_file = io.StringIO(coins_formatted_json) # Put ndjson into file 
    insert_data_into_BQ(coins_data_as_file)
    print("SUCCESSFULLY INSERTED GOOGLE COIN DATA INTO BQ")

# 2. Extract and upload data of 5 coin news googles to BQ
def extract_google_news_data_into_BQ(query_dict):
    news_google_json = get_search_metrics(query_dict)
    news_in_json = StringIO(news_google_json) # Format json to ndjson
    news_result = [json.dumps(record) for record in json.load(news_in_json)] 
    news_formatted_json = '\n'.join(news_result)
    news_data_as_file = io.StringIO(news_formatted_json) # Put ndjson into file 
    insert_data_into_BQ(news_data_as_file)
    print("SUCCESSFULLY INSERTED GOOGLE NEWS DATA INTO BQ")
