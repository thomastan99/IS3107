import os
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
  'owner' : 'airflow',
  'retries' : 2,
  'retry_delay' : timedelta(minutes=10)
}

# get dag directory path
dag_path = os.getcwd()

with DAG (
  dag_id='extract_transform_load_pipeline',
  default_args=default_args,
  description='This dag triggers the ETL pipeline for the IS3107 Project.',
  start_date=datetime(2023, 2, 13, 0),
  schedule='@daily' 
) as dag:
  
  # TODO: Edit this and import other 2 dags as subdags
  extract = PythonOperator(
    task_id='extract_google_search_coin_task',
  )
  
  extract