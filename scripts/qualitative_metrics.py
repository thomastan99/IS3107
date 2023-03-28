# pull data from big query
from google.cloud import bigquery
import os
# for data manipulations
import pandas as pd
import re
# for transformer models
import transformers
from transformers import (AutoTokenizer, AutoModelForSequenceClassification,TextClassificationPipeline)
import torch


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"
client = bigquery.Client()

    

def clean_text(x):
    """ function to lightly clean text data

    Args:
        x (str): social media text data

    Returns:
        str: lightly cleaned text data
    """
    x = x.encode('ascii', 'ignore').decode()  # remove unicode characters
    x = x.lower()
    x = re.sub(r'@(?:(?:[\w][\.]{0,1})*[\w]){1,29}', '', x) # username
    x = re.sub(r'https*\S+', ' ', x) # remove links
    x = re.sub(r'http*\S+', ' ', x)
    
    # light clean up of text
    x = re.sub(r'rt', ' ', x)
    x = re.sub(r'\'\w+', ' ', x) 
    x = re.sub(r'\w*\d+\w*', ' ', x)
    x = re.sub(r'\*|]\n', ' ', x)
    x = re.sub(r'\s{2,}', ' ', x)
    x = re.sub(r'\s[^\w\s]\s', ' ', x)
    
    return x.strip()

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

    
def pull_text_data(source):
    """
    extract relevant data from bigquery into dataframe
    """
    
    if 'realtime_tweets' in source:
        query = f"""
        with deduped_table as (
            select * from `crypto3107.{source}`
            )
        SELECT created_at AS date, text
        from deduped_table
        """
        results = client.query(query).to_dataframe()
        
    else:
        platform = source.split('.')[0] + "_details"
        
        query = f"""
        with deduped_table as (
        select `{platform}` from `crypto3107.{source}` limit 10
        )
        SELECT *
        from deduped_table
        
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
    
    # initialise pretrained model
    model_name = 'ElKulako/cryptobert' 
    print('Load Pretrained Model')
    model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=3)
    print('Successfully Load Pretrained Model')
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer, max_length=64, truncation=True, padding = 'max_length')
    
    # predict sentiment
    sentiment_test_lst = df['cleaned_text'].tolist()
    print('Predicting Sentiment Score')
    res = pipe(sentiment_test_lst)
    
    # output 
    df['bert_output'] = res
    df['score'] = df['bert_output'].apply(lambda x: x['score'])
    df['label'] = df['bert_output'].apply(lambda x: x['label'])
    df.drop(['bert_output'], inplace=True, axis=1)
    df['sentiment_score'] = df.apply(lambda x: transform_data(x), axis=1)
    print(f"Sentiment Score from CryptoBert:\n{df}")
    
    final_df = df.groupby('cleaned_date', as_index=True).agg( avg_sentiment_score=('sentiment_score', 'mean')).sort_values('cleaned_date', ascending=False)
    print(f"Daily Mean Sentiment Score:\n{final_df}")
    
    
predict_sentiment(data)