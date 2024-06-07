from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Emmanuel'
}

def print_function():
    print('This should be the simplest python operator ever!')

with DAG(
    dag_id = 'execute_python_operators',
    description = 'python operator in DAG',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= timedelta(days=1),
    tags = ['simple', 'python'],
) as dag:

    task_1 = PythonOperator(
        task_id = 'Task_1',
        python_callable= print_function
    )
    
    

task_1 