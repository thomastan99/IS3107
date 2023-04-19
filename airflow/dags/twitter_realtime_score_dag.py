import os
from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from extract.extract_realtime_twitter import \
    build_extract_twitter_realtime_coin_text_task
from load.load_realtime_sentiment_scores import \
    build_load_realtime_sentiment_scores_task
from transform.transform_realtime_twitter import \
    build_transform_realtime_sentiments

from airflow import DAG

default_args = {
  'owner' : 'airflow',
  'retries' : 2,
  'retry_delay' : timedelta(minutes=1)
}

with DAG (
  dag_id='twitter_realtime_score_dag',
  default_args=default_args,
  description='This dag triggers the process to generate realtime Twitter sentiment scores.',
  start_date=datetime(2023, 3, 16, 0),
  catchup=False,
  schedule='*/2 * * * *'
) as dag:
  start = EmptyOperator(task_id='start')
 
  with TaskGroup(group_id='extract') as extractGroup:
    extract_latest_twitter_text = build_extract_twitter_realtime_coin_text_task(dag=dag)
    
  with TaskGroup(group_id='transform') as transformGroup:
    transform_realtime_twitter = build_transform_realtime_sentiments(dag=dag)
  
  with TaskGroup(group_id='load') as loadGroup:
    load_model_output = build_load_realtime_sentiment_scores_task(dag=dag)
    
  
  end = EmptyOperator(task_id='end')
  
  start >> extractGroup >> transformGroup >> loadGroup >> end
