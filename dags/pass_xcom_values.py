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
    
def multiply_by_100(ti):
    value = ti.xcom_pull(task_ids='task_increment_by_1') #Pulls the value that was passed to xcom by the increment_by_1 task
    print('value: {value}!'.format(value=value))
    
    return value * 100


def subtract_9(ti):
    value = ti.xcom_pull(task_ids='task_multiply_by_100') #Pulls the value that was passed to xcom by multiply_by_100 task
    print('value: {value}!'.format(value=value))
    
    return value - 9


def print_value(ti):
    value = ti.xcom_pull(task_ids='task_subtract_9') #Pulls the value that was passed to xcom by subtract_9 task
    print('value: {value}!'.format(value=value))

    
with DAG(
    dag_id = 'Xcoms_values',
    description = 'Working with XCom values',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= timedelta(days=1),
    tags = ['XCom', 'values', 'python'],
) as dag:

    task_increment_by_1 = PythonOperator(
        task_id = 'task_increment_by_1',
        python_callable= increment_by_1,
        op_kwargs={'counter': 9}
    )
    
    task_multiply_by_100 = PythonOperator(
        task_id = 'task_multiply_by_100',
        python_callable= multiply_by_100,
    )
    
    task_subtract_9 = PythonOperator(
        task_id = 'task_subtract_9',
        python_callable= subtract_9,
    )
    
    task_print_value = PythonOperator(
        task_id = 'task_print_value',
        python_callable= print_value,
        op_kwargs={'counter': 100}
    )

task_increment_by_1 >> task_multiply_by_100 >> task_subtract_9 >> task_print_value

