from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Emmanuel'
}

with DAG(
    dag_id = 'multi_task_ops',
    description = 'multiple dags operations',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags = ['scheduled', 'once']
) as dag:

    task_1 = BashOperator(
        task_id = 'Task_1',
        bash_command= 'echo task one has been successfully executed',
    )
    
    task_2 = BashOperator(
        task_id = 'Task_2',
        bash_command= 'echo task two has been successfully executed',
    )

task_1.set_downstream(task_2)