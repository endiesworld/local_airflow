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
FILE_PATH = DIR_PATH.joinpath('datasets', 'insurance.csv')

OUTPUT_PATH = DIR_PATH.joinpath('output')

def read_csv_file_(ti):
    df = pd.read_csv(FILE_PATH)
    print(df)
    
    ti.xcom_push(key='my_csv', value=df.to_json())


def remove_null_values_(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='my_csv')
    
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    
    ti.xcom_push(key='my_clean_csv', value=df.to_json())


def determine_branch_():
    transform_action = Variable.get('transform_action', None)
    if transform_action:
        if transform_action.startswith('filter'):
            return 'filtering.{}'.format(transform_action)
        elif transform_action == 'groupby_region_smoker':
            return 'grouping.{}'.format(transform_action)
    
    
def filter_by_southwest_(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['region']== 'southwest']
    
    region_df.to_csv(OUTPUT_PATH.joinpath('southwest.csv'), index=False)
    

def filter_by_southeast_(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['region']== 'southeast']
    
    region_df.to_csv(OUTPUT_PATH.joinpath('southeast.csv'), index=False)
    
    
def filter_by_northwest_(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['region']== 'northwest']
    
    region_df.to_csv(OUTPUT_PATH.joinpath('northwest.csv'), index=False)
    
    
def filter_by_northeast_(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['region']== 'northeast']
    
    region_df.to_csv(OUTPUT_PATH.joinpath('northeast.csv'), index=False)
    
    
def groupby_region_smoker_(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    
    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean',
    }).reset_index()
    
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean',
    }).reset_index()
    
    region_df.to_csv(OUTPUT_PATH.joinpath('group_by_region.csv'), index=False)
    smoker_df.to_csv(OUTPUT_PATH.joinpath('group_by_smoker.csv'), index=False)


with DAG(
    dag_id = 'taskgroups_and_edgelabels',
    description = 'Runnign a branching pipeline with Taskgroups and edgelabels',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags = ['python', 'taskgroups', 'edgelabels', 'branching'],
) as dag:
    with TaskGroup('reading_and_preprocessing') as reading_and_preprocessing:
        read_csv_file = PythonOperator(
            task_id = 'read_csv_file',
            python_callable= read_csv_file_,
        )
        
        remove_null_values = PythonOperator(
            task_id = 'remove_null_values',
            python_callable = remove_null_values_
        )    
        
        read_csv_file >> remove_null_values
    
    determine_branch = BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable = determine_branch_
    )
    
    with TaskGroup('filtering') as filtering:
        filter_by_southwest = PythonOperator(
            task_id = 'filter_by_southwest',
            python_callable = filter_by_southwest_
        ) 
        
        filter_by_southeast = PythonOperator(
            task_id = 'filter_by_southeast',
            python_callable = filter_by_southeast_
        ) 
        
        filter_by_northwest = PythonOperator(
            task_id = 'filter_by_northwest',
            python_callable = filter_by_northwest_
        ) 
        
        filter_by_northeast = PythonOperator(
            task_id = 'filter_by_northeast',
            python_callable = filter_by_northeast_
        )
    
    with TaskGroup('grouping') as grouping:
        groupby_region_smoker = PythonOperator(
            task_id = 'groupby_region_smoker',
            python_callable = groupby_region_smoker_
        )
    
    
reading_and_preprocessing >> Label('proprocessed data') >> determine_branch >> Label('branch on condition') >> [filtering, grouping]

 