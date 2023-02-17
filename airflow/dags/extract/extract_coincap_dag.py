import os

from airflow.operators.python import PythonOperator

from airflow import DAG

# get dag directory path
dag_path = os.getcwd()

from scripts.ccxt_test import extract_coincap_api


def build_extract_coincap_task(dag: DAG) -> PythonOperator:
  extract_coincap_task = PythonOperator(
      task_id='extract_coincap_dag_task',
      python_callable=extract_coincap_api,
      op_kwargs={"dag_path": dag_path},
      dag=dag
    )
  
  return extract_coincap_task