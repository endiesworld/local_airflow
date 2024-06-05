from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Emmanuel'
}

with DAG(
    dag_id = 'hello_world_context_manager_dag',
    description = 'first local DAG operation with context manager',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@daily',
    tags = ['beginner', 'bash', 'hello world', 'context manager']
) as dag:

    task = BashOperator(
        task_id = 'hello_world_task_context_manager',
        bash_command= 'echo hello world from context manager dag',
    )

task