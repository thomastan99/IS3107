import datetime
import io
import json
import os
import sys
import timeit
from io import StringIO

import snscrape.modules.twitter as sntwitter
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

########################## ALL CREDENTIALS - REDDIT & BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

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
 
news_dict = {

      }

########################## METHOD TO SCRAPE DATA FROM TWITTER ##############################
def get_tweets_hashtags(query_dict): 
    twitter_data =[]
    
    def get_tweets_for_day(hashtag, limit_per_day, start_date, end_date, tweets_dict):
        count = 0    
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        query_result = sntwitter.TwitterSearchScraper(hashtag + f' since:{start_date_str} until:{end_date_str}').get_items()
        tic2 = timeit.default_timer() 
        for tweet in query_result:
            if count < limit_per_day:
                tweet_data = {}
                tweet_df = vars(tweet)
                
                tweet_data['public_metrics'] = {
                    'retweet_count': tweet_df['retweetCount'],
                    'reply_count': tweet_df['replyCount'],
                    'quote_count': tweet_df['quoteCount'],
                    'like_count': tweet_df['likeCount'],
                }
                tweet_data['id'] = tweet_df['id']
                tweet_data['text'] = tweet_df['rawContent']
                tweet_data['created_at'] = tweet_df['date'].strftime("%Y-%m-%dT%H:%M.%SSZ")
                tweets_dict['data'].append(tweet_data)
                
                print("hashtag, date, count ", hashtag, end_date_str, count)
                count += 1
            else:
                toc2 = timeit.default_timer() 
                print("time taken for date  ", end_date_str, toc2-tic2)
                break
        if (start_date <= end_date):
            get_tweets_for_day(hashtag, limit_per_day, start_date, end_date - datetime.timedelta(days=1), tweets_dict)
            return tweets_dict

    for hashtag in query_dict.keys():
        tweets_dict = {'data': []}        
        tic = timeit.default_timer() 
        tweets_dict = get_tweets_for_day(hashtag, 5, datetime.datetime.today() - datetime.timedelta(days=365*3), datetime.datetime.today(), tweets_dict)
        toc = timeit.default_timer()
        print("TIME TAKEN WHOLE PROCESS", (toc - tic)/60)

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
                            ]
                        ),
                    ]
                )
            ],
        )
    ]

    client = bigquery.Client()
    table_id = "crypto3107.training_data.twitter"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
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
def extract_tweet_coin_news_data_into_BQ(query_dict):
    coins_twitter_json = get_tweets_hashtags(query_dict)
    coins_in_json = StringIO(coins_twitter_json) # Format json to ndjson
    coins_result = [json.dumps(record) for record in json.load(coins_in_json)] 
    coins_formatted_json = '\n'.join(coins_result)
    coins_data_as_file = io.StringIO(coins_formatted_json) # Put ndjson into file 
    insert_data_into_BQ(coins_data_as_file)
    print("SUCCESSFULLY INSERTED TWITTER COIN AND NEWS DATA INTO BQ")



########################## TEMP TEST LINE FOR SCRAPING DATA ##############################
sys.setrecursionlimit(1000000)
extract_tweet_coin_news_data_into_BQ(coins_news_dict)


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