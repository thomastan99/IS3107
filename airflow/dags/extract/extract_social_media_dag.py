from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.google_search_script import get_search_metrics
from scripts.reddit_script import get_reddit_posts
from scripts.twitter_script import get_tweets_hashtags


def build_extract_social_media_task(dag: DAG) -> TaskGroup:
  dictionaries = {
    "google": {
      "method": get_search_metrics,
      "arg": "query_dict",
      "coins" :  {
        'bitcoin',
        'ethereum',
        'tether',
        'binance',
        'xrp'
      },
      "news" : {
        'nft',
        'cryptocurrency news',
        'buy cryptocurrency',
        'best cryptocurrency',
        'crypto exchange'
      }
    },
    "reddit": {
      "method": get_reddit_posts,
      "arg": "subreddit_dict",
      "coins" : {
        'bitcoin': 'r/Bitcoin',
        'ethereum': 'r/ethereum',
        'tether': 'r/Tether',
        'binance': 'r/binance',
        'xrp': 'r/XRP'
      },
      "news" : {
        'cryptocurrency': 'r/CryptoCurrency',
        'cryptomarkets': 'r/CryptoMarkets',
        'bitcoinbeginners': 'r/BitcoinBeginners',
        'cryptocurrencies': 'r/CryptoCurrencies',
        'crypto_general': 'r/Crypto_General'
      }
    },
    "twitter" : {
      "method": get_tweets_hashtags,
      "arg": "query_dict",
      "coins" : {
        '#bitcoin': '#bitcoin',
        '#ethereum': '#ethereum',
        '#tether': '#tether',
        '#binance': '#binance',
        '#xrp': '#xrp'
      }, 
      "news" : {
        '#crptomarket': '#crptomarket',
        '#cryptocurrency': '#cryptocurrency',
        '#crypto': '#crypto',
        '#cryptonews': '#cryptonews',
        '#blockchain': '#blockchain'
      }
    }
  }
  
  with TaskGroup(group_id='extract_social_media' ) as extractSocialMediaGroup:
    for socials in ['google', 'reddit', 'twitter']: 
      with TaskGroup(group_id=f'extract_{socials}') as path:
        social_dict = dictionaries.get(f'{socials}')
        
        coin_data = PythonOperator(
          task_id=f'extract_{socials}_search_coin_task',
          python_callable=social_dict.get('method'),
          op_kwargs={social_dict.get("arg"): social_dict.get('coins')},
          dag=dag
        )
        
        news_data = PythonOperator(
          task_id=f'extract_{socials}_search_news_task',
          python_callable=social_dict.get('method'),
          op_kwargs={social_dict.get("arg"): social_dict.get('news')},
          dag=dag
        )
        
        coin_data >> news_data
    
  return extractSocialMediaGroup
  
