import os
from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from extract.extract_quantitative import build_extract_quantitative_task
from extract.extract_social_media import build_extract_social_media_task
from load.load_model_output import build_load_model_output_task
from transform.transform_qualitative import build_transform_qualitative
from transform.transform_quantitative import build_transform_quantitative

from airflow import DAG

default_args = {
  'owner' : 'airflow',
  'retries' : 2,
  'retry_delay' : timedelta(minutes=1)
}

with DAG (
  dag_id='extract_transform_load_pipeline',
  default_args=default_args,
  description='This dag triggers the ETL pipeline for the IS3107 Project.',
  start_date=datetime(2023, 3, 16, 0),
  catchup=False,
  schedule='@daily' 
) as dag:
  start = EmptyOperator(task_id='start')
 
  with TaskGroup(group_id='extract') as extractGroup:
    extract_coincap = build_extract_quantitative_task(dag=dag)
    extract_social_media = build_extract_social_media_task(dag=dag)
    
  with TaskGroup(group_id='transform') as transformGroup:
    transform_quantitative = build_transform_quantitative(dag=dag)
    transform_qualitative = build_transform_qualitative(dag=dag)
    
    transform_qualitative >> transform_quantitative
  
  with TaskGroup(group_id='load') as loadGroup:
    load_model_output = build_load_model_output_task(dag=dag)
    
  
  end = EmptyOperator(task_id='end')
  
  start >> extractGroup >> transformGroup >> loadGroup >> end
