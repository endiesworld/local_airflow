import json
import csv
from pathlib import Path

import pandas as pd

from airflow.utils.dates import days_ago

from airflow.decorators import dag, task

from airflow.providers.sqlite.operators.sqlite import SqliteOperator

default_args = {
    'owner': 'Emmanuel'
}

DIR_PATH = Path.cwd()
FILE_PATH = DIR_PATH.joinpath('datasets', 'car_data.csv')

OUTPUT_PATH = DIR_PATH.joinpath('output')    

@dag(
    dag_id = 'data_transformation_storage_pipeline',
    description = 'Data transformation and storage pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags = ['python', 'sql', 'taskflow', 'xcoms'],
)
def data_transformation_storage_pipeline():
    @task
    def read_csv_file():
        df = pd.read_csv(FILE_PATH)
        print(df)
        
        return df.to_json()
    
    @task
    def create_table():
        create_table = SqliteOperator(
            task_id = 'create_table',
            sql= """
                    CREATE TABLE IF NOT EXISTS car_data(
                        id INTEGER PRIMARY KEY,
                        car_name VARCHAR(50) NOT NULL,
                        year INTEGER NOT NULL,
                        price FLOAT NOT NULL,
                        kms_driven INTEGER NOT NULL,
                        fuel_type VARCHAR(50) NOT NULL,
                        transmission VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """,
            sqlite_conn_id= 'my_sqlite_conn',
        )
        
        create_table.execute(context=None)
    
    @task
    def insert_selected_data(**kwargs):
        ti = kwargs['ti']
        
        json_data = ti.xcom_pull(task_ids='read_csv_file')
        
        df = pd.read_json(json_data)
        df = df[['Car_Name','Year', 'Selling_Price', 'Kms_Driven', 'Fuel_Type', 'Transmission']]
        df = df.applymap(lambda x: x.strip() if isinstance(x,str) else x)
        
        insert_query = """
            INSERT INTO car_data (car_name, year, price, kms_driven, fuel_type, transmission)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        datas = df.to_dict(orient='records')
        
        for data in datas:
            insert_values = SqliteOperator(
                task_id = 'insert_data',
                sql = insert_query,
                sqlite_conn_id= 'my_sqlite_conn',
                parameters = tuple(data.values())
            )
            insert_values.execute(context=None)
    
    read_csv_file() >> create_table() >> insert_selected_data()
  
    
    
data_transformation_storage_pipeline()
