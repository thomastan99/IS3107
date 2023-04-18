import io
import json
import os
import sys
import time
from datetime import datetime, timedelta
from io import StringIO

import requests
import snscrape.modules.twitter as sntwitter
from dateutil import tz
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

########################## ALL CREDENTIALS - BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

# email for disconnected stream
port = 465  
smtp_server = "smtp.gmail.com"
sender_email = "nus3107crypto@gmail.com"  
receiver_email = "nus3107crypto@gmail.com"  
password = "crypto12345"
message = """\
Subject: Twitter stream disconnected
Twitter stream has disconnected. Please reconnect."""

#OAuth explanation: https://developer.twitter.com/en/docs/tutorials/authenticating-with-twitter-api-for-enterprise/authentication-method-overview
cons_key = 'OrzYgeOiH3pAq1FzMj8ztxWKP' #INPUT CRED#
cons_secret = 'qPhRpzLu6nT2zbQGnW0ScDYYNVf4806cuzMae7EqD0AwZ9hFCM' #INPUT CRED#
acc_token = '130006594-9xx8NEPyImg7mTTYXvrXTPLu2Xxril614Mte7uWg' #INPUT CRED#
acc_secret = 'h9heI3YWGW0jtPiuLeI8EiCajmpX0H1Qg7RevgSnW07be' #INPUT CRED#
bear_token = "AAAAAAAAAAAAAAAAAAAAAAWMhAEAAAAAr9dE5XiFdQe1pWfLp7c65v3%2FuCM%3DSlCdglcfbIFnJLrLaKLy5NAJGXwKu2aRJ5JoMPGqQSoVXeCmpI" #INPUT CRED#

########################## GLOBAL VARIABLES ##############################
coins_news_dict = {
    '#bitcoin': '#bitcoin',
    '#ethereum': '#ethereum',
    '#xrp': '#xrp',        
    '#cryptomarket': '#crptomarket',
    '#cryptocurrency': '#cryptocurrency',
    '#crypto': '#crypto',
    '#cryptonews': '#cryptonews',
    '#blockchain': '#blockchain'
}


########################## METHOD TO SCRAPE DATA FROM TWITTER ##############################
def get_tweets_hashtags(query_dict): 
    twitter_data =[]
      
    def get_tweets_streaming(hashtag, count, count_limit, start_date, end_date, twitter_data):
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")        
        query_result = sntwitter.TwitterSearchScraper(hashtag + f' since:{start_date_str} until:{end_date_str}').get_items()

        for tweet in query_result:        
            if count < count_limit:
                tweet_data = {}
                tweet_df = vars(tweet)
                
                tweet_data['retweet_count'] = tweet_df['retweetCount']
                tweet_data['reply_count'] = tweet_df['replyCount']
                tweet_data['quote_count'] = tweet_df['quoteCount']
                tweet_data['like_count'] = tweet_df['likeCount']
                tweet_data['id'] = tweet_df['id']
                tweet_data['text'] = tweet_df['rawContent']
                tweet_data['tag'] = hashtag
                
                new_datetime = tweet_df['date'].strftime("%Y-%m-%d %H:%M:%S")
                utc = datetime.strptime(new_datetime, '%Y-%m-%d %H:%M:%S')
                utc = utc.replace(tzinfo=tz.gettz('UTC'))
                local = utc.astimezone(tz.gettz('Singapore'))
                local_dt = local.strftime('%Y-%m-%d %H:%M:%S')
                tweet_data['created_at'] = local_dt
                
                twitter_data.append(tweet_data)
                count += 1
            else:
                break
    for hashtag in query_dict.keys():       
        count = 0
        get_tweets_streaming(hashtag, count, 15, datetime.today(), datetime.today() + timedelta(days=1), twitter_data)
        
    tweets_json = json.dumps(twitter_data)
    return tweets_json


########################## METHOD TO UPLOAD DATA TO BQ ##############################

def insert_data_into_BQ(data_as_file) :
    schema = [
            bigquery.SchemaField("created_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tag", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("reply_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("quote_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("retweet_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("like_count", "INT64", mode="NULLABLE")
    ]

    client = bigquery.Client()
    table_id = "crypto3107.streaming.realtime_tweets"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
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

# 1. Extract and upload data of 3 coins and 5 news hashtags tweets to BQ
def extract_stream_data_into_gbq(query_dict):
    sys.setrecursionlimit(1000000000)
    while True:
        coins_twitter_json = get_tweets_hashtags(query_dict)
        coins_in_json = StringIO(coins_twitter_json) # Format json to ndjson
        coins_result = [json.dumps(record) for record in json.load(coins_in_json)] 
        coins_formatted_json = '\n'.join(coins_result)
        coins_data_as_file = io.StringIO(coins_formatted_json) # Put ndjson into file 
        insert_data_into_BQ(coins_data_as_file)
        print("SUCCESSFULLY INSERTED TWITTER COIN AND NEWS DATA INTO BQ")
        time.sleep(120)
