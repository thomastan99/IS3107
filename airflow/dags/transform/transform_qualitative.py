from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from scripts.qualitative_metrics import predict_sentiment, pull_text_data

# def pull_text_data_task(ti, source):
#     data, source = pull_text_data(source)
#     ti.xcom_push(key="data", value=data)
#     print("Data: ", data)


def predict_sentiment_task(ti, source):
    data = ti.xcom_pull(key="data", task_ids=f"pull_{source}_text_data")
    
    predict_sentiment(data, source)
    

def build_transform_qualitative(dag: DAG) -> PythonOperator:
  with TaskGroup(group_id='transform_qualitative_group') as transformQualitativeGroup:
    for source in ['twitter.batch_tweets', 'twitter.realtime_tweets']: 
      formatted_source_name = source.replace(".", "_")
      with TaskGroup(group_id=f'transform_{formatted_source_name}') as transform_source_data:
        pull_text_data_task  = PythonOperator(
          task_id=f'pull_{formatted_source_name}_text_data',
          python_callable=pull_text_data,
          op_kwargs={"source": source},
          dag=dag
        )
        
        # predict_sentiment  = PythonOperator(
        #   task_id=f'predict_{source}_sentiment',
        #   python_callable=predict_sentiment_task,
        #   op_kwargs={"source": source},
        #   dag=dag
        # )
        
        pull_text_data_task

  return transformQualitativeGroup
