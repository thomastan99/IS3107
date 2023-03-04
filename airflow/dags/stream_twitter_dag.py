from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from extract.extract_realtime_twitter_dag import \
    build_extract_twitter_realtime_task

from airflow import DAG

default_args = {
  'owner' : 'airflow',
  "email": ["ngcheeheng@u.nus.edu"],
  "email_on_failure": True,
  "email_on_retry": True,
  "retries": 1,
  "retry_delay": timedelta(minutes=1),
}

with DAG (
  dag_id='stream_twitter_dag',
  default_args=default_args,
  description='This dag triggers the continuous streaming pipeline for the realtime Twitter Data.',
  start_date=datetime(2023, 3, 4, 0),
) as dag:  
  start = EmptyOperator(task_id='start')
    
  extract_realtime_twitter = build_extract_twitter_realtime_task(dag=dag)

  start >> extract_realtime_twitter  