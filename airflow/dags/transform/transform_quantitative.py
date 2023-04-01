from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG


def build_transform_quantitative(dag: DAG) -> PythonOperator:
  with TaskGroup(group_id='transform_quantitative_group') as transformQuantitativeGroup:
    for coin in ['bitcoin_combined', 'ethereum_combined', 'xrp_combined']:     
      transform_quantitative_task = PapermillOperator(
        task_id=f"transform_{coin}_notebook",
        input_nb=f"scripts/model_{coin}_v2.ipynb",
        output_nb=f"scripts/output_model_{coin}_v2.ipynb",
      )
      
      transform_quantitative_task

  return transformQuantitativeGroup

