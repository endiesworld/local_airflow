import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Emmanuel'
}

def increment_by_1(counter):
    print('counter: {counter}!'.format(counter=counter))
    return counter + 1
    
def multiply_by_100(counter):
    print('counter: {counter}!'.format(counter=counter))
    return counter * 100
    
with DAG(
    dag_id = 'cross_task_comms',
    description = 'Cross-task communication with XCom',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= timedelta(days=1),
    tags = ['XCom', 'python'],
) as dag:

    taskA = PythonOperator(
        task_id = 'taskA',
        python_callable= increment_by_1,
        op_kwargs={'counter': 9}
    )
    
    taskB = PythonOperator(
        task_id = 'taskB',
        python_callable= multiply_by_100,
        op_kwargs={'counter': 100}
    )
    

taskA >> taskB

