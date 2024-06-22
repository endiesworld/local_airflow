from pathlib import Path

import pandas as pd

from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label


default_args = {
    'owner': 'Emmanuel'
}

DIR_PATH = Path.cwd()
FILE_PATH = DIR_PATH.joinpath('datasets', 'car_data.csv')

OUTPUT_PATH = DIR_PATH.joinpath('output')

def read_csv_file_(ti):
    df = pd.read_csv(FILE_PATH)
    print(df)
    
    ti.xcom_push(key='my_car_csv', value=df.to_json())


def remove_null_values_(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='my_car_csv')
    
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    
    ti.xcom_push(key='my_clean_car_csv', value=df.to_json())


def determine_branch_():
    transform_action = Variable.get('transform', None)
    if transform_action:
        if transform_action == 'filter_fuel':
            return 'filter_petrol_task'
        elif transform_action == 'filter_transmission':
            return 'filter_transmission_task'
    
    
def filter_fuel_(ti):
    json_data = ti.xcom_pull(key='my_clean_car_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['Fuel_Type']== 'Petrol']
    
    region_df.to_csv(OUTPUT_PATH.joinpath('petrol_car_data.csv'), index=False)
    

def filter_transmission_(ti):
    json_data = ti.xcom_pull(key='my_clean_car_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['Transmission']== 'Manual']
    
    region_df.to_csv(OUTPUT_PATH.joinpath('manual_car_data.csv'), index=False)
    


with DAG(
    dag_id = 'branching_using_operators',
    description = 'Branching using operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags = ['python', 'operators', 'branching'],
) as dag:
    read_csv_file = PythonOperator(
        task_id = 'read_csv_file',
        python_callable= read_csv_file_,
    )
        
    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_null_values_
    )    
    
    determine_branch = BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable = determine_branch_
    )
    
    filter_fuel = PythonOperator(
        task_id = 'filter_fuel',
        python_callable = filter_fuel_
    ) 
    
    filter_transmission = PythonOperator(
        task_id = 'filter_transmission',
        python_callable = filter_transmission_
    ) 
        
read_csv_file >> remove_null_values >> determine_branch >> [filter_fuel, filter_transmission]
 