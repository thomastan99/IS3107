from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.twitter_realtime_script import \
    extract_twitter_realtime_coin_news_data


def build_extract_twitter_realtime_task(dag: DAG) -> TaskGroup:  
  with TaskGroup(group_id='extract_twitter_realtime') as extractRealTimeTwitter:
    twitter_realtime_coin_data = PythonOperator(
      task_id=f'extract_twitter_real_time_coins_news_twitter',
      python_callable=extract_twitter_realtime_coin_news_data,
      dag=dag
    ) 
    
  return extractRealTimeTwitter
  
