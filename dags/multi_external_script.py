from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Emmanuel'
}

with DAG(
    dag_id = 'exter_file_task_ops_2',
    description = 'multiple dags operations wuth exter script file',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= timedelta(days=1),
    tags = ['bitshift', 'external'],
    template_searchpath = '/home/endie/Projects/Data_Engineering/airflow_prject_3/dags/bash_scripts'
) as dag:

    task_1 = BashOperator(
        task_id = 'Task_1',
        bash_command= "taskA.sh"
    )
    
    task_2 = BashOperator(
        task_id = 'Task_2',
        bash_command= "taskB.sh",
    )
    
    task_3 = BashOperator(
        task_id = 'Task_3',
        bash_command= "taskC.sh",
    )
    
    task_4 = BashOperator(
        task_id = 'Task_4',
        bash_command= "taskD.sh",
    )
    
    task_5 = BashOperator(
        task_id = 'Task_5',
        bash_command= "taskE.sh",
    )
    
    task_6 = BashOperator(
        task_id = 'Task_6',
        bash_command= "taskF.sh",
    )
    
    task_7 = BashOperator(
        task_id = 'Task_7',
        bash_command= "taskG.sh",
    )

task_1 >> task_2 >> task_5

task_1 >> task_3 >> task_6

task_1 >> task_4 >> task_7