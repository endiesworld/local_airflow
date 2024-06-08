import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Emmanuel'
}

def task_a():
    print('Task A executed!')
    

def task_b():
    time.sleep(5)
    print('Task B executed!')
    
    
def task_c():
    print('Task C executed!')
    
    
def task_d():
    print('Task D executed!')

with DAG(
    dag_id = 'execute_python_operators',
    description = 'python operator in DAG',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= timedelta(days=1),
    tags = ['simple', 'python'],
) as dag:

    taskA = PythonOperator(
        task_id = 'taskA',
        python_callable= task_a
    )
    
    taskB = PythonOperator(
        task_id = 'taskB',
        python_callable= task_b
    )
    
    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable= task_c
    )
    
    taskD = PythonOperator(
            task_id = 'taskD',
            python_callable= task_d
        )

taskA >> [taskB, taskC] >> taskD
