import io
import json
import os
from io import StringIO

import praw
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta  
import datetime as dt
from dateutil import tz

########################## ALL CREDENTIALS - REDDIT & BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

client_id = 'uwjwEuIgxWDsluEe4bgAew' 
client_secret = 'x9j8g9e5MGGKvxJqnCmfQUioxS3pKA' 
user_agent = 'crypto3107' 
reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)

########################## METHOD TO EXTRACT DATA FROM REDDIT ##############################

def get_reddit_posts(subreddit_dict): 
    subreddit_data = []
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Singapore')

    for subreddit in subreddit_dict.keys(): 
        hot_posts = reddit.subreddit(subreddit).hot(limit=21)
        subreddit_details = []
        removePinned = False
        for post in hot_posts:
            if removePinned != False:
                # Convert UTC to GMT+8
                new_datetime = str(dt.datetime.fromtimestamp(post.created))
                utc = datetime.strptime(new_datetime, '%Y-%m-%d %H:%M:%S')
                utc = utc.replace(tzinfo=from_zone)
                local = utc.astimezone(to_zone)
                local_dt = local.strftime('%Y-%m-%d %H:%M:%S')

                postDetails = {'title':post.title, 'text':post.selftext, 'author':str(post.author),
                               'number_comments':post.num_comments,'number_upvotes':post.score, 'created_at': local_dt}
                subreddit_details.append(postDetails)
            else: 
                removePinned = True
        reddit_data = []
        reddit_data.append({'data': subreddit_details})
        subreddit_data.append({'reddit': subreddit_dict[subreddit], 'reddit_details': reddit_data})
        
    
    reddit_json = json.dumps(subreddit_data)
    return reddit_json

########################## METHOD TO UPLOAD DATA TO BQ ##############################

def insert_data_into_BQ(data_as_file) :
    schema = [
        bigquery.SchemaField("reddit", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "reddit_details",
            "RECORD",
            mode = "REPEATED",
            fields=[
                bigquery.SchemaField(
                    "data",
                    "RECORD",
                    mode = "REPEATED",
                    fields=[
                        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("author", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("number_comments", "INT64", mode="NULLABLE"),
                        bigquery.SchemaField("number_upvotes", "INT64", mode="NULLABLE"),
                        bigquery.SchemaField("created_at", "STRING", mode="NULLABLE"),
                    ]
                )
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
#         "data" : [
    #         {
    #             "title": "title",
    #             "text": "text",
    #             "author": "author",
    #             "number_comments": "number_comments",
    #             "number_upvotes": "number_upvotes",
    #             "created_at": "datetime"
    #         }
#         ]
#     ]
# }]
########################################################