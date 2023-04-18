from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.twitter_dashboarding_extraction import pull_twitter_text
from scripts.twitter_realtime_script import extract_stream_data_into_gbq


def build_extract_twitter_realtime_task(dag: DAG) -> TaskGroup:  
  with TaskGroup(group_id='extract_twitter_realtime') as extractRealTimeTwitter:
    coins_news_dict = {
        '#bitcoin': '#bitcoin',
        '#ethereum': '#ethereum',
        '#xrp': '#xrp',        
        '#cryptomarket': '#crptomarket',
        '#cryptocurrency': '#cryptocurrency',
        '#crypto': '#crypto',
        '#cryptonews': '#cryptonews',
        '#blockchain': '#blockchain'
    }
    twitter_realtime_coin_data = PythonOperator(
      task_id=f'extract_twitter_real_time_coins_news_twitter',
      python_callable=extract_stream_data_into_gbq,
      op_kwargs={'query_dict': coins_news_dict},
      dag=dag
    ) 
    
  return extractRealTimeTwitter

def extract_twitter_coin_text_data(ti, coin_name):
  results, local_end_datetime, local_start_datetime = pull_twitter_text(coin_name)
  ti.xcom_push(key="results", value=results)
  ti.xcom_push(key="start_time", value=local_start_datetime)
  ti.xcom_push(key="end_time", value=local_end_datetime)
  

def build_extract_twitter_realtime_coin_text_task(dag: DAG) -> TaskGroup:
  with TaskGroup(group_id='extract_twitter_realtime_coin_text') as extractRealTimeTwitterText:
    coin_list = ["ethereum", "bitcoin", "xrp"]
    for coin in coin_list:
      twitter_realtime_coin_data = PythonOperator(
        task_id=f'extract_twitter_{coin}_text',
        python_callable=extract_twitter_coin_text_data,
        op_kwargs={'coin_name': coin},
        do_xcom_push=True,
        dag=dag
      ) 
  
  return extractRealTimeTwitterText
