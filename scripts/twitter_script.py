import io
import json
import os
import sys
from io import StringIO

import requests
import tweepy
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from tweepy import Cursor, OAuthHandler

########################## ALL CREDENTIALS - REDDIT & BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

cons_key = 'OrzYgeOiH3pAq1FzMj8ztxWKP' #INPUT CRED#
cons_secret = 'qPhRpzLu6nT2zbQGnW0ScDYYNVf4806cuzMae7EqD0AwZ9hFCM' #INPUT CRED#
acc_token = '130006594-9xx8NEPyImg7mTTYXvrXTPLu2Xxril614Mte7uWg' #INPUT CRED#
acc_secret = 'h9heI3YWGW0jtPiuLeI8EiCajmpX0H1Qg7RevgSnW07be' #INPUT CRED#
bear_token = "AAAAAAAAAAAAAAAAAAAAAAWMhAEAAAAAr9dE5XiFdQe1pWfLp7c65v3%2FuCM%3DSlCdglcfbIFnJLrLaKLy5NAJGXwKu2aRJ5JoMPGqQSoVXeCmpI" #INPUT CRED#

########################## GLOBAL VARIABLES ##############################

# top 10 crypto coins hashtags
coins_dict = {
    '#bitcoin': '#bitcoin',
    '#ethereum': '#ethereum',
    '#tether': '#tether',
    '#binance': '#binance',
    '#binance': '#xrp',
    '#cardano': '#cardano',
    '#polygon': '#polygon',
    '#dogecoin': '#dogecoin',
    '#solana': '#solana',
    '#polkadot': '#polkadot'
}

# top 5 crypto news hashtags
news_dict = {
    '#cryptomarket': '#crptomarket',
    '#cryptocurrency': '#cryptocurrency',
    '#crypto': '#crypto',
    '#cryptonews': '#cryptonews',
    '#blockchain': '#blockchain'
}

########################## METHOD TO EXTRACT DATA FROM TWITTER ##############################

def get_twitter_client():

    try:
        consumer_key = cons_key
        consumer_secret = cons_secret
        access_token = acc_token
        access_secret = acc_secret
        bearer_token = bear_token
    except KeyError:
        sys.stderr.write("Twitter Environment Variable not Set\n")
        sys.exit(1)
        
    client = tweepy.Client(bearer_token=bearer_token, 
                        consumer_key=consumer_key, 
                        consumer_secret=consumer_secret, 
                        access_token=access_token, 
                        access_token_secret=access_secret, 
                        return_type = requests.Response,
                        wait_on_rate_limit=True)
    return client

########################## METHOD TO EXTRACT DATA FROM TWITTER ##############################

def get_tweets_hashtags(query_dict): 
    client = get_twitter_client()
    twitter_data =[]
    for hashtag in query_dict.keys():
        # get recent tweets via search query terms, return [10-100] results
        # tweet_fields: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet 
        # Other types of fields: https://docs.tweepy.org/en/stable/expansions_and_fields.html#tweet-fields
        tweets = client.search_recent_tweets(query=hashtag, 
                                            tweet_fields = ['created_at','text', 'public_metrics'],
                                            max_results=100)
        tweets_dict = tweets.json() 
        del tweets_dict['meta']
        for item in tweets_dict.get('data'):
            del item['edit_history_tweet_ids']
            
        twitter_data.append({'twitter': query_dict[hashtag], 'tweets_details': tweets_dict})
    
    tweets_json = json.dumps(twitter_data, indent=4)
    return tweets_json

########################## METHOD TO UPLOAD DATA TO BQ ##############################

def insert_data_into_BQ(data_as_file) :
    schema = [
        bigquery.SchemaField("twitter", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "tweets_details",
            "RECORD",
            mode = "REPEATED",
            fields=[
                bigquery.SchemaField(
                    "data",
                    "RECORD",
                    mode = "REPEATED",
                    fields=[
                        bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("created_at", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField(
                            "public_metrics", 
                            "RECORD", 
                            mode="REPEATED", 
                            fields = [
                                bigquery.SchemaField("retweet_count", "INT64", mode="NULLABLE"),
                                bigquery.SchemaField("reply_count", "INT64", mode="NULLABLE"),
                                bigquery.SchemaField("like_count", "INT64", mode="NULLABLE"),
                                bigquery.SchemaField("quote_count", "INT64", mode="NULLABLE"),
                                bigquery.SchemaField("impression_count", "INT64", mode="NULLABLE"),
                            ]
                        ),
                    ]
                )
            ],
        )
    ]

    client = bigquery.Client()
    table_id = "crypto3107.twitter.batch_tweets"
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

# 1. Extract and upload data of 10 coins tweets to BQ
def extract_tweet_coin_data_into_BQ(query_dict):
    coins_twitter_json = get_tweets_hashtags(query_dict)
    coins_in_json = StringIO(coins_twitter_json) # Format json to ndjson
    coins_result = [json.dumps(record) for record in json.load(coins_in_json)] 
    coins_formatted_json = '\n'.join(coins_result)
    coins_data_as_file = io.StringIO(coins_formatted_json) # Put ndjson into file 
    insert_data_into_BQ(coins_data_as_file)
    print("SUCCESSFULLY INSERTED TWITTER COIN DATA INTO BQ")

# 2. Extract and upload data of 5 coin news tweets to BQ
def extract_tweet_news_data_into_BQ(query_dict):
    news_twitter_json = get_tweets_hashtags(query_dict)
    news_in_json = StringIO(news_twitter_json) # Format json to ndjson
    news_result = [json.dumps(record) for record in json.load(news_in_json)] 
    news_formatted_json = '\n'.join(news_result)
    news_data_as_file = io.StringIO(news_formatted_json) # Put ndjson into file 
    insert_data_into_BQ(news_data_as_file)
    print("SUCCESSFULLY INSERTED TWITTER NEWS DATA INTO BQ")


########################################################
# JSON Structure: 
# [{
#     "twitter": "#hashtag",
#     "tweets_details": {
#         "data" : [
#             {
#                 "id": "1627601344667189249",
#                 "created_at":"date, time",
#                 "text": "text",
#                 "edit_history_tweet_ids": ["1627601344667189249"],
#                 "public_metrics": {
#                     "retweet_count": "",
#                     "reply_count": "",
#                     "like_count": "",
#                     "quote_count": "",
#                     "impression_count":""
#                 }
#             }
#         ]
# }]
########################################################