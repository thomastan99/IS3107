from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from scripts.quantitative_metrics import pull_coin_data


def build_transform_quantitative(dag: DAG) -> PythonOperator:
  with TaskGroup(group_id='transform_quantitative_group') as transformQuantitativeGroup:
    for coin in ['bitcoin_combined', 'ethereum_combined', 'xrp_combined']: 
      transform_quantitative_task  = PythonOperator(
        task_id=f'transform_quantitative_{coin}_dag_task',
        python_callable=pull_coin_data,
        op_kwargs={"coin_name": coin},
        dag=dag
      )
      
      transform_quantitative_task

  return transformQuantitativeGroup

