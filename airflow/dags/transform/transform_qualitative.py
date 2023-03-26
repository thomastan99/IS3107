from airflow.operators.python import PythonOperator

from airflow import DAG
from scripts.ccxt_test import extract_coincap_api


def build_transform_qualitative(dag: DAG) -> PythonOperator:
  transform_qualitative_task = PythonOperator(
      task_id='transform_qualitative_dag_task',
      python_callable=extract_coincap_api,
      dag=dag
    )
  
  return transform_qualitative_task
