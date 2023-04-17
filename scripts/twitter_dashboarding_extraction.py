import os
from datetime import datetime, timedelta

import pandas as pd
from dateutil import tz
from google.cloud import bigquery
from nltk.sentiment.vader import SentimentIntensityAnalyzer

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

def pull_twitter_text(tag):
    client = bigquery.Client()

    query = f"""
    with deduped_table as (
        select * from crypto3107.streaming.realtime_tweets
        )
    SELECT d.created_at AS date, d.text
    from deduped_table d, UNNEST(matching_rules) AS nested_column 
    WHERE nested_column.tag LIKE @coin AND TIMESTAMP(d.created_at) >= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -2 MINUTE);
    """

    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('coin', 'STRING', f'%{tag}%')]) 
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Generate time of query in GMT
    end_datetime = datetime.strptime(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    local_end_datetime = end_datetime.replace(tzinfo=tz.gettz('UTC')).astimezone(tz.gettz('Singapore')).strftime('%Y-%m-%d %H:%M:%S')
    
    start_datetime = datetime.strptime(datetime.strftime((end_datetime - timedelta(minutes=2)), '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    local_start_datetime = start_datetime.replace(tzinfo=tz.gettz('UTC')).astimezone(tz.gettz('Singapore')).strftime('%Y-%m-%d %H:%M:%S')
        
    results.date = pd.to_datetime(results.date)
    
    print("coin, start, end", tag, local_start_datetime, local_end_datetime)
    print("results",results)
    return results, local_end_datetime, local_start_datetime


def generate_realtime_sentiment_score(df, start_time, end_time):
    """
    create a new sentiment feature to be fed into ML model
    Cryptobert pretrained model predicts three classes
    {
    "Bearish": 0,
    "Bullish": 2,
    "Neutral": 1
    }
    
    """
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.set_index('date')
    # Creating the sentiment analyzer object
    sid = SentimentIntensityAnalyzer()
    
    def score_tweets(tweets):
        # Determing the sentiment and sentiment score for each of the tweets.
        sentiment = []
        score = []
        for tweet in tweets:
            ss = sid.polarity_scores(str(tweet))
            score.append(ss['compound'])
    
        return score

    score = score_tweets(df.text)    

    df['score'] = score
    num_tweets = len(df.index)
    # make a time series of mean score per day
    ts = df.groupby(pd.Grouper(key='date', freq='D')).mean().reset_index()
    ts["start_time"] = start_time
    ts["end_time"] = end_time
    ts["num_tweets"] = num_tweets
    ts.drop(columns=['date'])
    return ts
 

def load_score_into_gbq(coin, scores):
    client = bigquery.Client()
    table_id = f'crypto3107.streaming.{coin}_score'
    
    schema = [
        bigquery.SchemaField("start_time", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("end_time", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("score", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("num_tweets", "INT64", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(schema=schema)
    
    client.load_table_from_dataframe(scores, table_id, job_config=job_config)

