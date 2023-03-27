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
        platform = source.split('.')[0] + "_details"
        
        query = f"""
        with deduped_table as (
        select `{platform}` from `crypto3107.{source}` limit 10
        )
        SELECT double_nested_column.text, CAST(double_nested_column.created_at AS TIMESTAMP) as date
        FROM deduped_table as d, UNNEST(tweets_details) AS nested_column, UNNEST(nested_column.data) AS double_nested_column
        WHERE d.twitter LIKE @coin AND CAST(double_nested_column.created_at AS TIMESTAMP) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR));
        """
        raw_data = client.query(query).to_dataframe()
        cols = ['text', 'date']
        text_col = []
        date_col = [ ]
        for row in raw_data[platform]: # remove range to load all
            sub_rows = row[0]['data']
            for idx in range(len(sub_rows)):
                details = sub_rows[idx]
                text_data = details['text']
                date_data = details['created_at']
                text_col.append(text_data)
                date_col.append(date_data)
        results = pd.DataFrame(list(zip(text_col,date_col)), columns=cols)
    
    print(results['date'].max())
    print(results['date'].min())
        
    results['cleaned_text'] = results['text'].apply(clean_text)
    results["cleaned_date"]= pd.to_datetime(results['date']).dt.date

    print(f"Cleaned Text Data:\n{results}")
    return results[['cleaned_text', 'cleaned_date']]

data = pull_text_data("reddit.batch_posts") # or "twitter.realtime_tweets" or "twitter.batch_tweets"


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


