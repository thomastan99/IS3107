import io
import json
import os
from io import StringIO

import praw
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.oauth2 import service_account

########################## ALL CREDENTIALS - REDDIT & BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

client_id = 'uwjwEuIgxWDsluEe4bgAew' 
client_secret = 'x9j8g9e5MGGKvxJqnCmfQUioxS3pKA' 
user_agent = 'crypto3107' 
reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)

########################## GLOBAL VARIABLES ##############################

# top 10 crypto coins subreddit
coins_dict = {
    'bitcoin': 'r/Bitcoin',
    'ethereum': 'r/ethereum',
    'tether': 'r/Tether',
    'binance': 'r/binance',
    'xrp': 'r/XRP',
    'cardano': 'cardano',
    'polygon': 'r/polygon',
    'dogecoin': 'r/dogecoin',
    'solana': 'r/solana',
    'polkadot': 'r/polkadot'
}

# top 5 crypto news subreddit - can use for justification: https://coinbound.io/best-crypto-subreddits/ 
news_dict = {
    'cryptocurrency': 'r/CryptoCurrency',
    'cryptomarkets': 'r/CryptoMarkets',
    'bitcoinbeginners': 'r/BitcoinBeginners',
    'cryptocurrencies': 'r/CryptoCurrencies',
    'crypto_general': 'r/Crypto_General'
}

########################## METHOD TO EXTRACT DATA FROM REDDIT ##############################

def get_reddit_posts(subreddit_dict): 
    subreddit_data = []

    for subreddit in subreddit_dict.keys(): 
        hot_posts = reddit.subreddit(subreddit).hot(limit=21)
        subreddit_details = []
        removePinned = False
        for post in hot_posts:
            if removePinned != False:
                postDetails = {'title':post.title, 'text':post.selftext, 'author':str(post.author),'number_comments':post.num_comments,'number_upvotes':post.score}
                subreddit_details.append(postDetails)
            else: 
                removePinned = True
        subreddit_data.append({'subreddit': subreddit_dict[subreddit], 'subreddit_details': subreddit_details})
        
    
    reddit_json = json.dumps(subreddit_data)
    return reddit_json

########################## METHOD TO UPLOAD DATA TO BQ ##############################

def insert_data_into_BQ(data_as_file) :
    schema = [
        bigquery.SchemaField("subreddit", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "subreddit_details",
            "RECORD",
            mode = "REPEATED",
            fields=[
                bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("author", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("number_comments", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("number_upvotes", "INT64", mode="NULLABLE"),
            ],
        )
    ]

    client = bigquery.Client()
    table_id = "crypto3107.reddit.batch_posts"
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

# 1. Extract and upload data of 10 coins subreddits to BQ
def extract_reddit_coin_data_into_BQ(query_dict):
    coins_reddit_json = get_reddit_posts(query_dict)
    coins_in_json = StringIO(coins_reddit_json) # Format json to ndjson
    coins_result = [json.dumps(record) for record in json.load(coins_in_json)] 
    coins_formatted_json = '\n'.join(coins_result)
    coins_data_as_file = io.StringIO(coins_formatted_json) # Put ndjson into file 
    insert_data_into_BQ(coins_data_as_file)
    print("SUCCESSFULLY INSERTED REDDIT COIN DATA INTO BQ")

# 2. Extract and upload data of 5 coin news subreddits to BQ
def extract_reddit_news_data_into_BQ(query_dict):
    news_reddit_json = get_reddit_posts(query_dict)
    news_in_json = StringIO(news_reddit_json) # Format json to ndjson
    news_result = [json.dumps(record) for record in json.load(news_in_json)] 
    news_formatted_json = '\n'.join(news_result)
    news_data_as_file = io.StringIO(news_formatted_json) # Put ndjson into file 
    insert_data_into_BQ(news_data_as_file)
    print("SUCCESSFULLY INSERTED REDDIT NEWS DATA INTO BQ")




########################################################
# JSON Structure: 
# [{
#     "subreddit": "r/subreddit",
#     "subreddit_details": [
#         {
#             "title": "title",
#             "text": "text",
#             "author": "author",
#             "number_comments": "number_comments",
#             "number_upvotes": "number_upvotes"
#         }
#     ]
# }]
########################################################