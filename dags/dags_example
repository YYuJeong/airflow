from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 17),
    'catchup': False,
}

dag = DAG(
    dag_id='simple_dummy_dag',  # DAG ID
    default_args=default_args,
    schedule_interval='@daily',  # 매일 실행
    tags=['example'],
)

# DummyOperator 사용
task_start = DummyOperator(
    task_id='start',
    dag=dag,
)

task_end = DummyOperator(
    task_id='end',
    dag=dag,
)

# 작업 순서 정의
task_start >> task_end
