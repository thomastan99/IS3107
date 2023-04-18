import os

import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.oauth2 import service_account
from nltk.sentiment.vader import SentimentIntensityAnalyzer

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

def insert_data_into_BQ(coin_name):
    schema = [
        bigquery.SchemaField("priceUsd", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date", "STRING", mode="NULLABLE"),
    ]

    client = bigquery.Client(location="asia-southeast1")
    table_id = f"crypto3107.coincap.{coin_name}"
    table_exists = False
    tables = client.list_tables('coincap')
    for table in tables:
        if table.table_id == coin_name:
            table_exists = True
            break

    # If the table does not exist, create it
    if not table_exists:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # API request
        print(f'Table {table.table_id} was created.')
    
    with open(f'./assets/coincap_{coin_name}_data.json', 'rb') as f:
        job_config = bigquery.LoadJobConfig(
            source_format='NEWLINE_DELIMITED_JSON',
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

def push_to_gbq(coin):
    import datetime

    import pandas as pd
    import yfinance as yf

    start_date = datetime.date.today() - datetime.timedelta(days=3*365)
    end_date = datetime.date.today()

    def get_data(coin):
        data = yf.download(coin, start=start_date, end=end_date, interval="1d")
        data = data.rename(columns= {'Adj Close': "Price"})
        data = data.reset_index(drop=False).rename(columns={'index': 'Date'})
        data = data.reset_index(drop=True)
        return data
    ticker = ""
    if coin == "ethereum":
        ticker = 'ETH-USD'
    elif coin == "bitcoin":
        ticker = 'BTC-USD'
    elif coin == "xrp":
        ticker = 'XRP-USD'
    df = get_data(ticker)
    table_id = f'crypto3107.yfinance.{coin}'
    creds = service_account.Credentials.from_service_account_file('./creds/cred.json')
    df.to_gbq(table_id, location="asia-southeast1", reauth=False, project_id='crypto3107', if_exists='replace', credentials=creds)



def load_model_output_into_BQ(coin) :
    client = bigquery.Client()
    table_id = f"crypto3107.model_output.{coin}"
    job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.CSV, 
      skip_leading_rows=1, 
      autodetect=True,
      write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    with open(f"assets/model_output/model_output_{coin}.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    
    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def pull_coin_data(coin_name):
    client = bigquery.Client()

    query = f"""
    with deduped_table as (
    select distinct * from `crypto3107.yfinance.{coin_name}_combined`
    )
    SELECT *
    from deduped_table

    order by Date desc

    """

    results = client.query(query).to_dataframe()
    results.Date = pd.to_datetime(results.Date)
    outname = f'quantitative_data_{coin_name}.csv'

    outdir = 'airflow/assets/quantitative/'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    results.to_csv(f'assets/quantitative/{outname}')






def pull_text_data(source, tag):
    client = bigquery.Client()
    """
    extract relevant data from bigquery into dataframe
    """
    
    if 'realtime_tweets' in source:
        query = f"""
        with deduped_table as (
            select * from `crypto3107.twitter.{source}`
            )
        SELECT d.created_at AS date, d.text
        from deduped_table d, UNNEST(matching_rules) AS nested_column 
        WHERE nested_column.tag LIKE @coin AND DATE(REPLACE(LEFT(d.created_at, 19), 'T', ' ')) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        """
        
    else:
        query = f"""
        with twitter_table as (
        select twitter, tweets_details from `crypto3107.{source}.twitter`
        )
        SELECT double_nested_column.text, DATE(LEFT(double_nested_column.created_at, 10)) AS date
        FROM twitter_table as d, UNNEST(tweets_details) AS nested_column, UNNEST(nested_column.data) AS double_nested_column
        WHERE d.twitter LIKE @coin AND DATE(LEFT(double_nested_column.created_at, 10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
        UNION ALL
        SELECT double_nested_column.title as text, DATE(double_nested_column.created_at) AS date
        FROM `crypto3107.{source}.reddit` as r, UNNEST(reddit_details) AS nested_column, UNNEST(nested_column.data) AS double_nested_column
        WHERE r.reddit LIKE @coin AND DATE(double_nested_column.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
        """

    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('coin', 'STRING', f'%{tag}%')]) 
    results = client.query(query, job_config=job_config).to_dataframe()
    if source == 'realtime_tweets':
        results.date = results.date.apply(lambda x: x[0:10])
    results.date = pd.to_datetime(results.date)
      
    # Handle file naming for reddit 
    if 'r/' in tag:
        tag = tag.replace('r/', 'r_')
    if source == 'realtime_tweets':
        tag = 'realtime_twitter_' + tag
    
    outname = f'qualitative_data_{tag}.csv'

    outdir = 'airflow/assets/qualitative/extract/'
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    fullname = os.path.join(outdir, outname) 
    results.to_csv(f'assets/qualitative/extract/{outname}', index=False)




def predict_sentiment(filepath):
    """
    create a new sentiment feature to be fed into ML model
    Cryptobert pretrained model predicts three classes
    {
    "Bearish": 0,
    "Bullish": 2,
    "Neutral": 1
    }
    
    """
    df = pd.read_csv(f'assets/qualitative/extract/{filepath}', engine='python')
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
    # make a time series of mean score per day
    ts = df.groupby(pd.Grouper(key='date', freq='D')).mean().reset_index()
    new_filepath = filepath.replace('qualitative_data', 'score')
    
    outname = f'qualitative_data_{new_filepath}'

    outdir = 'airflow/assets/qualitative/score/'
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    fullname = os.path.join(outdir, outname) 
    ts.to_csv(f'assets/qualitative/score/{outname}', index=False)
 


