

import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.twitter_dashboarding_extraction import \
    generate_realtime_sentiment_score


def transform_realtime_twitter_score(ti, coin):
    results = ti.xcom_pull(key="results", task_ids=f"extract.extract_twitter_realtime_coin_text.extract_twitter_{coin}_text")
    local_start_datetime = ti.xcom_pull(key="start_time", task_ids=f"extract.extract_twitter_realtime_coin_text.extract_twitter_{coin}_text")
    local_end_datetime = ti.xcom_pull(key="end_time", task_ids=f"extract.extract_twitter_realtime_coin_text.extract_twitter_{coin}_text")
    
    results = pd.read_json(results, orient='columns')
    
    scores = generate_realtime_sentiment_score(results, local_start_datetime, local_end_datetime)  
    print("SCORES", scores)
    scores = scores.to_json()
    ti.xcom_push(key="scores", value=scores)
    

def build_transform_realtime_sentiments(dag: DAG) -> PythonOperator:
  with TaskGroup(group_id='transform_realtime_twitter_score') as transformRealtimeSentimentGroup:
    coin_list = ["ethereum", "bitcoin", "xrp"]
    for coin in coin_list:
      predict_realtime_sentiment_task  = PythonOperator(
        task_id=f'predict_realtime_{coin}_sentiment',
        python_callable=transform_realtime_twitter_score,
        op_kwargs={"coin": coin},
        do_xcom_push=True,
        dag=dag
      ) 

  return transformRealtimeSentimentGroup
