from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator

# 실행할 Python 함수 정의
def print_hello():
    print("Hello, Airflow!")  # 실행 시 "Hello, Airflow!" 메시지를 출력

# DAG (Directed Acyclic Graph) 정의
with DAG(
    dag_id="python_operator_example",  # DAG의 고유한 식별자
    schedule_interval=None,  # 스케줄링을 사용하지 않음 (수동 실행)
    start_date=datetime.datetime(2021, 1, 1),  # DAG 시작 날짜 설정
) as dag:
    
    # PythonOperator를 사용한 Task 정의
    hello_task = PythonOperator(
        task_id='hello_task',  # Task의 고유한 ID
        python_callable=print_hello  # 실행할 Python 함수 지정
    )
    
    # Task 실행 흐름 정의 (현재는 단일 Task이므로 별도 의존성 없음)
    hello_task
