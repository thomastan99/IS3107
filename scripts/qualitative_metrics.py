# pull data from big query
from google.cloud import bigquery
import os
# for data manipulations
import pandas as pd
# sentiment analysis
from nltk.sentiment.vader import SentimentIntensityAnalyzer



os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"
client = bigquery.Client()

 
def transform_data(row):
    """transform sentiment score from -1 to 1

    Args:
        row (float): positive sentiment score for each tweet

    Returns:
        float: rescale sentiment score from -1 to 1 
    """
    if row['label'] == 'Bullish':
        return row['score'] * 1
    elif row['label'] == 'Bearish':
        return row['score'] * -1
    return row['score'] * 0

    
def pull_text_data(source, coin):
    """
    extract relevant data from bigquery into dataframe
    """
    
    if 'realtime_tweets' in source:
        query = f"""
        with deduped_table as (
            select * from `crypto3107.{source}`
            )
        SELECT d.created_at AS date, d.text
        from deduped_table d, UNNEST(matching_rules) AS nested_column 
        WHERE nested_column.tag LIKE @coin AND CAST(d.created_at AS TIMESTAMP) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));
        """
        
    else:
        query = f"""
        with deduped_table as (
        select twitter, tweets_details from `crypto3107.{source}`
        )
        SELECT double_nested_column.text, CAST(double_nested_column.created_at AS TIMESTAMP) as date
        FROM deduped_table as d, UNNEST(tweets_details) AS nested_column, UNNEST(nested_column.data) AS double_nested_column
        WHERE d.twitter LIKE @coin AND CAST(double_nested_column.created_at AS TIMESTAMP) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR));
        """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('coin', 'STRING', f'%{coin}%')]) 
    results = client.query(query, job_config=job_config).to_dataframe()
    return results #results[['cleaned_text', 'cleaned_date']]

data = pull_text_data("twitter.batch_tweets", "#bitcoin") # or "twitter.realtime_tweets/ batch_tweets"


def predict_sentiment(df):
    """
    create a new sentiment feature to be fed into ML model
    Cryptobert pretrained model predicts three classes
    {
    "Bearish": 0,
    "Bullish": 2,
    "Neutral": 1
    }
    
    """
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
    # make a time series of mean score per day
    ts = df.groupby(pd.Grouper(key='date', freq='D')).mean().reset_index()
    print(ts)
    return ts
 
final_df = predict_sentiment(data)
final_df.to_csv('qualitative_data_sample.csv', index=False)


