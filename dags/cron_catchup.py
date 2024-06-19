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
Cron Expression:
    A cron expression is a string consisting of six or seven subexpressions (fields) that describe individual details of the schedule.
    These fields, separated by white space.
    Cron expressions can be as simple as * * * * ? * or as complex as 0 0/5 14,18,3-39,52 ? JAN,MAR,SEP MON-FRI 2002-2010.
examples:
    0 0 12 * * ?	    Fire at 12:00 PM (noon) every day
    0 15 10 ? * *	    Fire at 10:15 AM every day
    0 15 10 * * ?	    Fire at 10:15 AM every day
    0 15 10 * * ? *	    Fire at 10:15 AM every day
    0 15 10 * * ? 2005	Fire at 10:15 AM every day during the year 2005
    * * * * *           Every minute
    0 * * * *           Every hour
    0 0 * * *           Every day at 12:00 AM
    0 0 * * FRI         At 12:00 AM, only on Friday
    0 */12 * * 6,0      Every twelve hour interval for the 6th day(Saturday) and first day of the week(Sunday)
    Minute, Hour, Day of month, Month, Year.
    To generate more expression https://crontab.cronhub.io/
    
"""

with DAG(
    dag_id = 'cron_catchup_backfill',
    description = 'Using crons, catchup, and backfill',
    default_args = default_args,
    start_date = days_ago(5),
    schedule_interval= '0 0 * * *',
    catchup = True,
    tags = ['catchup', 'backup', 'cron', 'pipeline'],
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

