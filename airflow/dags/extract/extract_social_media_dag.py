import os
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.google_search_script import get_search_metrics

# from scripts.reddit_script import get_reddit_posts
# from scripts.telegram_script import get_telegram_channel
# from scripts.twitter_script import get_tweets_hashtags

# get dag directory path
dag_path = os.getcwd()

# Google
# top 5 crypto coins search terms
google_coins_dict = [
    'bitcoin',
    'ethereum',
    'tether',
    'binance',
    'xrp'
]

# top 5 crypto news search terms
google_news_dict = {
    'nft',
    'cryptocurrency news',
    'buy cryptocurrency',
    'best cryptocurrency',
    'crypto exchange'
}

# Reddit
# top 5 crypto coins subreddit
reddit_coins_dict = {
    'bitcoin': 'r/Bitcoin',
    'ethereum': 'r/ethereum',
    'tether': 'r/Tether',
    'binance': 'r/binance',
    'xrp': 'r/XRP'
}

# top 5 crypto news subreddit
# can use for justification: https://coinbound.io/best-crypto-subreddits/ 
reddit_news_dict = {
    'cryptocurrency': 'r/CryptoCurrency',
    'cryptomarkets': 'r/CryptoMarkets',
    'bitcoinbeginners': 'r/BitcoinBeginners',
    'cryptocurrencies': 'r/CryptoCurrencies',
    'crypto_general': 'r/Crypto_General'
}

# Telegram
# top 5 largest crypto telegram channels
tele_crypto_dict = {
    'binanceexchange': '@binanceexchange',
    'CryptoWorldNews': '@CryptoWorldNews',
    'bitcoin_industry': '@bitcoin_industry',
    'crypto_mountains': '@crypto_mountains',
    'DeFimillion': '@DeFimillion'
}

# Twitter
# top 5 crypto coins hashtags
twitter_coins_dict = {
    '#bitcoin': '#bitcoin',
    '#ethereum': '#ethereum',
    '#tether': '#tether',
    '#binance': '#binance',
    '#xrp': '#xrp'
}

# top 5 crypto news hashtags
twitter_news_dict = {
    '#crptomarket': '#crptomarket',
    '#cryptocurrency': '#cryptocurrency',
    '#crypto': '#crypto',
    '#cryptonews': '#cryptonews',
    '#blockchain': '#blockchain'
}


#   googleCoinTask = PythonOperator(
#     task_id='extract_google_search_coin_task',
#     python_callable=get_search_metrics,
#     op_kwargs={"query_dict": google_coins_dict}
#   )
  
#   googleNewsTask = PythonOperator(
#     task_id='extract_google_search_news_task',
#     python_callable=get_search_metrics,
#     op_kwargs={"query_dict": google_news_dict}
#   )
  
# redditCoinTask = PythonOperator(
#   task_id='extract_reddit_search_coin_task',
#   python_callable=get_reddit_posts,
#   op_kwargs={"subreddit_dict": reddit_coins_dict}
# )

# redditNewsTask = PythonOperator(
#   task_id='extract_reddit_search_news_task',
#   python_callable=get_reddit_posts,
#   op_kwargs={"subreddit_dict": reddit_news_dict}
# )

# telegramTask = PythonOperator(
#   task_id='extract_telegram_search_task',
#   python_callable=get_telegram_channel,
#   op_kwargs={"channel_dict": tele_crypto_dict}
# )

# twitterCoinTask = PythonOperator(
#   task_id='extract_google_search_coin_task',
#   python_callable=get_tweets_hashtags,
#   op_kwargs={"query_dict": twitter_coins_dict}
# )

# twitterNewsTask = PythonOperator(
#   task_id='extract_google_search_news_task',
#   python_callable=get_tweets_hashtags,
#   op_kwargs={"query_dict": twitter_news_dict}
# )
  
def build_extract_social_media_task(dag: DAG) -> TaskGroup:
  with TaskGroup(group_id='extract_social_media' ) as extractSocialMediaGroup:
    googleCoinTask = PythonOperator(
      task_id='extract_google_search_coin_task',
      python_callable=get_search_metrics,
      op_kwargs={"query_dict": google_coins_dict},
      dag=dag
    )
    
    googleNewsTask = PythonOperator(
      task_id='extract_google_search_news_task',
      python_callable=get_search_metrics,
      op_kwargs={"query_dict": google_news_dict},
      dag=dag
    )
    
    googleCoinTask >> googleNewsTask
  
  return extractSocialMediaGroup
  
