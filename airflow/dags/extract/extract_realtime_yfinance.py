from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from scripts.yfinance_realtime_script import extract_yfinance_realtime_data


def build_extract_yfinance_realtime_task(dag: DAG) -> TaskGroup:  
  with TaskGroup(group_id='extract_yfinance_realtime') as extractRealTimeYfinance:
    tickers = ["ETH-USD", "BTC-USD", "XRP-USD"]
    for ticker in tickers: 
      twitter_realtime_coin_data = PythonOperator(
        task_id=f'extract_realtime_{ticker}_yfinance',
        python_callable=extract_yfinance_realtime_data,
        op_kwargs={'ticker': ticker},
        dag=dag
      ) 
    
  return extractRealTimeYfinance
  
