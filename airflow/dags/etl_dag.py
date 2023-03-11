import os
from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from extract.extract_coincap_dag import build_extract_coincap_task
from extract.extract_social_media_dag import build_extract_social_media_task

from airflow import DAG

default_args = {
  'owner' : 'airflow',
  'retries' : 2,
  'retry_delay' : timedelta(minutes=10)
}

with DAG (
  dag_id='extract_transform_load_pipeline',
  default_args=default_args,
  description='This dag triggers the ETL pipeline for the IS3107 Project.',
  start_date=datetime(2023, 2, 13, 0),
  schedule='@daily' 
) as dag:
  
  # get dag directory path
  dag_path = os.getcwd()
  
  start = EmptyOperator(task_id='start')
 
  with TaskGroup(group_id='extract') as extractGroup:
    extract_coincap = build_extract_coincap_task(dag=dag, dag_path=dag_path)
    extract_social_media = build_extract_social_media_task(dag=dag)

  start >> extractGroup
