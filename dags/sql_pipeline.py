import time
from pathlib import Path

import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.providers.sqlite.operators.sqlite import SqliteOperator

default_args = {
    'owner': 'Emmanuel'
}


with DAG(
    dag_id = 'sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags = ['python', 'sql', 'pipeline'],
) as dag:

    create_table = SqliteOperator(
        task_id = 'create_table',
        sql= r"""
                CREATE TABLE IF NOT EXISTS users(
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    age INTEGER NOT NULL,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
        sqlite_conn_id= 'my_sqlite_conn',
    )
    
    insert_values_1 = SqliteOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users (name, age) VALUES
                ('Emmanuel', 36),
                ('Adaobi', 31),
                ('Chidubem', 12)
                ('Sochikamuma', 4)
        """,
        sqlite_conn_id= 'my_sqlite_conn',
    )
    
    insert_values_2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users (name, age) VALUES
                ('Amarachi', 34),
                ('Ugochukwu', 32),
                ('Chinaenye', 28)
                ('Favour', 23)
        """,
        sqlite_conn_id= 'my_sqlite_conn',
    )
    
    insert_values_2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users (name, age) VALUES
                ('Amarachi', 34),
                ('Ugochukwu', 32),
                ('Chinaenye', 28)
                ('Favour', 23)
        """,
        sqlite_conn_id= 'my_sqlite_conn',
    )
    
    display_result = SqliteOperator(
        task_id = 'display_result',
        sql = r"SELECT * FROM users",
        sqlite_conn_id= 'my_sqlite_conn',
        do_xcom_push = True
    )
    
    
create_table >> [insert_values_1, insert_values_2] >> display_result

