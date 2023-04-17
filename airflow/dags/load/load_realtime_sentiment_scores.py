import os

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.twitter_dashboarding_extraction import load_score_into_gbq


def load_scores_into_BQ(ti, coin):
    scores = ti.xcom_pull(key="scores", task_ids=f"transform.transform_realtime_twitter_score.predict_realtime_{coin}_sentiment")
    load_score_into_gbq(coin, scores)
    

def build_load_realtime_sentiment_scores_task(dag: DAG) -> TaskGroup:
  coins_lst = ['bitcoin', 'ethereum', 'xrp']
  with TaskGroup(group_id='load_realtime_sentiment_scores') as loadRealtimeSentimentScoresGroup:
    for coin in coins_lst: 
      load_realtime_sentiment_scores_task = PythonOperator(
          task_id=f'load_{coin}_model_output_task',
          python_callable=load_scores_into_BQ,
          op_kwargs={'coin': coin},        
          do_xcom_push=True,
          dag=dag
        )
  
  return loadRealtimeSentimentScoresGroup
