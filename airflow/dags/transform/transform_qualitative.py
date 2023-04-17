from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.helper_functions import predict_sentiment


def build_transform_qualitative(dag: DAG) -> PythonOperator:
  filenames = {
    'common_bitcoin': 'qualitative_data_bitcoin.csv',
    'common_ethereum': 'qualitative_data_ethereum.csv',
    'common_xrp': 'qualitative_data_xrp.csv',
    'reddit_cryptocurrency': 'qualitative_data_r_CryptoCurrency.csv',
    'reddit_cryptomarkets': 'qualitative_data_r_CryptoMarkets.csv',
    'reddit_bitcoinbeginners': 'qualitative_data_r_BitcoinBeginners.csv',
    'reddit_cryptocurrencies': 'qualitative_data_r_CryptoCurrencies.csv',
    'reddit_crypto_general': 'qualitative_data_r_Crypto_General.csv',
    'twitter_cryptomarket': 'qualitative_data_#crptomarket.csv',
    'twitter_cryptocurrency': 'qualitative_data_#cryptocurrency.csv',
    'twitter_crypto': 'qualitative_data_#crypto.csv',
    'twitter_cryptonews': 'qualitative_data_#cryptonews.csv',
    'twitter_blockchain': 'qualitative_data_#blockchain.csv',
    'twitter_realtime_bitcoin': 'qualitative_data_realtime_twitter_bitcoin.csv',
    'twitter_realtime_ethereum': 'qualitative_data_realtime_twitter_ethereum.csv',
    'twitter_realtime_xrp': 'qualitative_data_realtime_twitter_xrp.csv',
    'twitter_realtime_cryptomarket': 'qualitative_data_realtime_twitter_#crptomarket.csv',
    'twitter_realtime_cryptocurrency': 'qualitative_data_realtime_twitter_#cryptocurrency.csv',
    'twitter_realtime_crypto': 'qualitative_data_realtime_twitter_#crypto.csv',
    'twitter_realtime_cryptonews': 'qualitative_data_realtime_twitter_#cryptonews.csv',
    'twitter_realtime_blockchain': 'qualitative_data_realtime_twitter_#blockchain.csv'
  }
  
  with TaskGroup(group_id='transform_qualitative_group') as transformQualitativeGroup:
    for filename in filenames.keys():   
      predict_sentiment_task  = PythonOperator(
        task_id=f'predict_{filename}_sentiment',
        python_callable=predict_sentiment,
        op_kwargs={"filepath": f'{filenames[filename]}'},
        dag=dag
      ) 

  return transformQualitativeGroup
