from random import choice
from datetime import datetime, timedelta

from airflow import DAG

from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'Emmanuel'
}

def choose_branch():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='taskChoose'):
        return 'taskC'
    else:
        return 'taskD'
    
def task_c():
    print("TASK C executed!")

"""
Backfill:
    Executing past DAG runs for a specific date range rather than all DAG runs.
    Backfill is the process of running a dag or specific task in a dag for the past days. 
    For example, if a dag has been running since the start of a month and a new task was added to it,
    and this newly added task needs to be executed for the past days, you need to backfill it.
    In airflow, the backfill command will re-run all the instances of the dag_id for all the intervals 
    within the specified start and end date. It is also possible to re-run specific tasks within a dag.
Command: The backfill operation is executed using airflow terminal commands.
>> airflow dags backfill -s <my start date 2024-01-01> -e <my end date 2024-06-19> <my_dag_id cron_backfill>
"""

with DAG(
    dag_id = 'cron_backfill',
    description = 'Using crons, catchup, and backfill',
    default_args = default_args,
    start_date = days_ago(30),
    schedule_interval= '0 */12 * * 6,0',
    catchup = False,
    tags = ['backfill', 'backup', 'cron', 'pipeline'],
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command= 'echo TASK A has been executed',
    )
    
    taskChoose = PythonOperator(
        task_id = 'taskChoose',
        python_callable = choose_branch
    )    
    
    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable= branch
    )
    
    taskC = BranchPythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )
    
    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D has executed!'
    )
    
    taskE = EmptyOperator(
        task_id = 'taskE',
    )
    
    
taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD

