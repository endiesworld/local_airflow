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
        bash_command= """
            echo TASK_1 has started!
            
            for i in {1..10}
            do
                echo TASK_1 printing $i
            done
            
            echo TASK_1 has finished!
        """
    )
    
    task_2 = BashOperator(
        task_id = 'Task_2',
        bash_command= """
            echo TASK_2 has started!
            sleep 4
            echo TASK_2 has finished!
        """,
    )
    
    task_4 = BashOperator(
        task_id = 'Task_4',
        bash_command= "echo TASK_4 has finished!",
    )
    
    task_3 = BashOperator(
        task_id = 'Task_3',
        bash_command= """
            echo TASK_3 has started!
            sleep 15
            echo TASK_3 has finished!
        """,
    )

task_1 >> (task_2)
task_1 >> (task_3)

task_4 << (task_2)
task_4 << (task_3)