import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Emmanuel'
}

def greet_hello(name):
    print('Hello, {name}!'.format(name=name))
    
def greet_hello_with_city(name, city):
    print('Hello, {name} from {city}!'.format(name=name, city=city))
    
with DAG(
    dag_id = 'python_ops_with_args',
    description = 'python operator in DAG',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= timedelta(days=1),
    tags = ['parameters', 'python'],
) as dag:

    taskA = PythonOperator(
        task_id = 'taskA',
        python_callable= greet_hello,
        op_kwargs={'name': 'Emmanuel'}
    )
    
    taskB = PythonOperator(
        task_id = 'taskB',
        python_callable= greet_hello_with_city,
        op_kwargs={'name': 'Okoro', 'city': 'Ikeja'}
    )
    

taskA >> taskB
