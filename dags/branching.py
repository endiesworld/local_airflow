from random import choice

from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator

default_args = {
    'owner': 'Emmanuel'
}

def has_driving_license():
    return choice([True, False])


def branch_(**kwargs):
    ti = kwargs['ti']
    if ti.xcom_pull(task_ids='has_driving_license'):
        return 'eligible_to_drive'
    
    return 'not_eligible_to_drive'


def eligible_to_drive():
    print('You can drive, you have a license!')
    
    
def not_eligible_to_drive():
    print("I'm afraid you are out of luck, you need a license to drive !")


with DAG(
    dag_id = 'branching_pipeline',
    description = 'Runnign a branching pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags = ['python', 'branch', 'pipeline'],
) as dag:

    taskA = PythonOperator(
        task_id = 'has_driving_license',
        python_callable= has_driving_license,
    )
    
    taskB = PythonOperator(
        task_id = 'eligible_to_drive',
        python_callable = eligible_to_drive
    )    
    
    taskC = PythonOperator(
        task_id = 'not_eligible_to_drive',
        python_callable = not_eligible_to_drive
    )
    
    branch = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch_
    )
    
    
taskA >> branch >> [taskB, taskC]

