import os

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.load_model_outputs import load_model_output_into_BQ


def build_load_model_output_task(dag: DAG) -> TaskGroup:
  coins_lst = ['bitcoin', 'ethereum', 'xrp']
  with TaskGroup(group_id='load_model_output') as loadModelOutputGroup:
    for coin in coins_lst: 
      load_model_output_task = PythonOperator(
          task_id=f'load_{coin}_model_output_task',
          python_callable=load_model_output_into_BQ,
          op_kwargs={'coin': coin},
          dag=dag
        )
  
  return loadModelOutputGroup
