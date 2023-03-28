import datetime
import io
import json
import os
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
                    'retweet_count': tweet_df.get('retweetCount'),
                    'reply_count': tweet_df.get('replyCount'),
                    'quote_count': tweet_df.get('quoteCount'),
                    'impression_count': tweet_df.get('viewCount')
                }
                tweet_data['id'] = tweet_df.get('id')
                tweet_data['text'] = tweet_df.get('rawContent')
                tweet_data['created_at'] = tweet_df.get('datetime')
                tweets_dict['data'].append(tweet_data)
                
                print("date count ", end_date_str, count)
                count += 1
            else:
                toc2 = timeit.default_timer() 
                print("time taken for date  ", end_date_str, toc2-tic2)
                break
        if (start_date <= end_date):
            get_tweets_for_day(hashtag, limit_per_day, start_date, end_date - datetime.timedelta(days=1), tweets_dict)
        else:
            return tweets_dict
    
    for hashtag in query_dict.keys():
        tweets_dict = {'data': []}        
        tic = timeit.default_timer() 
        tweets_dict = get_tweets_for_day(hashtag, 5, datetime.datetime.today() - datetime.timedelta(days=365*3), datetime.datetime.today(), tweets_dict)
        toc = timeit.default_timer()
        print('tweets', tweets_dict)
        print("TIME TAKEN WHOLE PROCESS", (tic - toc)/60)
            
        twitter_data.append({'twitter': query_dict[hashtag], 'tweets_details': tweets_dict})
        twitter_data.json()
    
    tweets_json = json.dumps(twitter_data, indent=4)
    print("tweets json", tweets_json)
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



########################## TEMP TEST LINE FOR SCRAPING DATA ##############################
get_tweets_hashtags({'#bitcoin': '#bitcoin'})


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