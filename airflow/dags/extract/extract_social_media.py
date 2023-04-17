from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.helper_functions import pull_text_data



def build_extract_social_media_task(dag: DAG) -> TaskGroup:
  tags = {
    'common_bitcoin': 'bitcoin',
    'common_ethereum': 'ethereum',
    'common_xrp': 'xrp',
    'reddit_cryptocurrency': 'r/CryptoCurrency',
    'reddit_cryptomarkets': 'r/CryptoMarkets',
    'reddit_bitcoinbeginners': 'r/BitcoinBeginners',
    'reddit_cryptocurrencies': 'r/CryptoCurrencies',
    'reddit_crypto_general': 'r/Crypto_General',
    'twitter_cryptomarket': '#crptomarket',
    'twitter_cryptocurrency': '#cryptocurrency',
    'twitter_crypto': '#crypto',
    'twitter_cryptonews': '#cryptonews',
    'twitter_blockchain': '#blockchain'
  }
  
  with TaskGroup(group_id='extract_social_media') as extractSocialMediaGroup:
    for tag in tags.keys(): 
      extract_social_media_dag = PythonOperator(
          task_id=f'extract_training_data_{tag}_task',
          python_callable=pull_text_data, 
          op_kwargs={'source': 'training_data', 'tag': tags[tag]},
          dag=dag
        )
      if ('common' in tag or 'twitter' in tag):
        extract_realtime_twitter_dag = PythonOperator(
            task_id=f'extract_realtime_twitter_{tag}_task',
            python_callable=pull_text_data,
            op_kwargs={'source': 'realtime_tweets', 'tag': tags[tag]},
            dag=dag
          )
  return extractSocialMediaGroup
  
