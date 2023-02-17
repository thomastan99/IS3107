import os
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

# get dag directory path
dag_path = os.getcwd()

from scripts.ccxt_test import extract_coincap_api

default_args = {
  'owner' : 'airflow',
  'retries' : 2,
  'retry_delay' : timedelta(minutes=10)
}

with DAG (
  dag_id='extract_coincap_dag_v01',
  default_args=default_args,
  description='This dag triggers the extraction of data for the top 10 coins through daily running of the file ccxt_test.py.',
  start_date=datetime(2023, 2, 13, 0),
  schedule='@daily' 
) as dag:
  task1 = PythonOperator(
    task_id='extract_coincap_dag_task',
    python_callable=extract_coincap_api,
    op_kwargs={"dag_path": dag_path}
  )
  
  task1