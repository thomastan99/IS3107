import os

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.quantitative_metrics import pull_coin_data


def build_extract_quantitative_task(dag: DAG) -> TaskGroup:
  coins_lst = ['bitcoin', 'ethereum', 'xrp']
  with TaskGroup(group_id='extract_quantitative') as extractQuantitativeGroup:
    for coin in coins_lst: 
      extract_quantitative_task = PythonOperator(
          task_id=f'extract_quantitative_{coin}_task',
          python_callable=pull_coin_data,
          op_kwargs={'coin_name': coin},
          dag=dag
        )
  
  return extractQuantitativeGroup
