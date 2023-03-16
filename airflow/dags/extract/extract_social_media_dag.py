from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.google_search_script import (extract_google_coin_data_into_BQ,
                                          extract_google_news_data_into_BQ)
from scripts.reddit_script import (extract_reddit_coin_data_into_BQ,
                                   extract_reddit_news_data_into_BQ)
from scripts.twitter_script import (extract_tweet_coin_data_into_BQ,
                                    extract_tweet_news_data_into_BQ)


def build_extract_social_media_task(dag: DAG) -> TaskGroup:
  dictionaries = {  
    "reddit": {
      "coinMethod": extract_reddit_coin_data_into_BQ,
      "newsMethod": extract_reddit_news_data_into_BQ,
      "arg": "query_dict",
      "coins" : {
        'bitcoin': 'r/Bitcoin',
        'ethereum': 'r/ethereum',
        'tether': 'r/Tether',
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
      "coinMethod": extract_tweet_coin_data_into_BQ,
      "newsMethod": extract_tweet_news_data_into_BQ,
      "arg": "query_dict",
      "coins" : {
        '#bitcoin': '#bitcoin',
        '#ethereum': '#ethereum',
        '#tether': '#tether',
      }, 
      "news" : {
        '#cryptocurrency': '#cryptocurrency',
        '#crypto': '#crypto',
        '#cryptonews': '#cryptonews',
        '#blockchain': '#blockchain'
      }
    }
  }
  
  with TaskGroup(group_id='extract_social_media' ) as extractSocialMediaGroup:
    for socials in ['reddit', 'twitter']: 
      with TaskGroup(group_id=f'extract_{socials}') as path:
        social_dict = dictionaries.get(f'{socials}')
        
        coin_data = PythonOperator(
          task_id=f'extract_{socials}_search_coin_task',
          python_callable=social_dict.get('coinMethod'),
          op_kwargs={social_dict.get("arg"): social_dict.get('coins')},
          dag=dag
        )
        
        news_data = PythonOperator(
          task_id=f'extract_{socials}_search_news_task',
          python_callable=social_dict.get('newsMethod'),
          op_kwargs={social_dict.get("arg"): social_dict.get('news')},
          dag=dag
        )
        
        coin_data >> news_data
    
  return extractSocialMediaGroup
  
