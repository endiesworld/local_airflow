from pathlib import Path

import pandas as pd

from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.decorators import dag, task


default_args = {
    'owner': 'Emmanuel'
}

DIR_PATH = Path.cwd()
FILE_PATH = DIR_PATH.joinpath('datasets', 'car_data.csv')

OUTPUT_PATH = DIR_PATH.joinpath('output')    


@dag(
    dag_id = 'branching_using_taskflow_api',
    description = 'Branching using operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags = ['taskflow', 'python', 'branching'],
)
def branching_using_taskflow():
    @task(task_id='read_csv_file')
    def read_csv_file_():
        df = pd.read_csv(FILE_PATH)
        print(df)
        
        return df.to_json()
    
    
    @task(task_id='remove_null_val')
    def remove_null_values_(**kwarg):
        ti = kwarg['ti']
        json_data = ti.xcom_pull(task_ids = 'read_csv_file')
        
        df = pd.read_json(json_data)
        df = df.dropna()
        print(df)
        
        return df.to_json()    


    @task.branch
    def determine_branch_():
        transform_action = Variable.get('transform_fuel', None)
        if transform_action:
            if transform_action == 'filter_fuel':
                return 'filter_petrol_task'
            elif transform_action == 'filter_transmission':
                return 'filter_transmission_task'
    
    
    @task(task_id='filter_petrol')
    def filter_petrol_(**kwarg):
        ti = kwarg['ti']
        json_data = ti.xcom_pull(task_ids = 'remove_null_val')
        df = pd.read_json(json_data)
        
        region_df = df[df['Fuel_Type']== 'Petrol']
        
        region_df.to_csv(OUTPUT_PATH.joinpath('petrol_car_data.csv'), index=False)
    
    
    @task(task_id='filter_transmission')
    def filter_transmission_(**kwarg):
        ti = kwarg['ti']
        json_data = ti.xcom_pull(task_ids = 'remove_null_val')
        df = pd.read_json(json_data)
        
        region_df = df[df['Transmission']== 'Manual']
        
        region_df.to_csv(OUTPUT_PATH.joinpath('manual_car_data.csv'), index=False)
        
    
        
    read_csv_file_() >> remove_null_values_() >> determine_branch_() >> [filter_petrol_(), filter_transmission_()]
 
branching_using_taskflow()