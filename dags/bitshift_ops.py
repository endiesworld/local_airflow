from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Emmanuel'
}

with DAG(
    dag_id = 'bitshift_multi_task_ops_2',
    description = 'improved multiple dags operations mwith bitshift',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= timedelta(days=1),
    tags = ['bitshift']
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