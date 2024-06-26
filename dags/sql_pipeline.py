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
                    city VARCHAR(50),
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
        sqlite_conn_id= 'my_sqlite_conn',
    )
    
    insert_values_1 = SqliteOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users (name, age, is_active) VALUES
                ('Emmanuel', 36, true),
                ('Adaobi', 31, true),
                ('Chidubem', 12, true),
                ('Sochikamuma', 4, true),
                ('Chukwudalu', 0, true),
                ('Somtochi', 0, false)
        """,
        sqlite_conn_id= 'my_sqlite_conn',
    )
    
    insert_values_2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users (name, age, is_active) VALUES
                ('Amarachi', 34, true),
                ('Ugochukwu', 32, true),
                ('Chinaenye', 28, true),
                ('Favour', 23, true),
                ('Lucy', 28, true),
                ('Ugo_nephew', 0, false)
        """,
        sqlite_conn_id= 'my_sqlite_conn',
    )
    
    delete_values = SqliteOperator(
        task_id = 'delete_values',
        sql = r""" DELETE FROM users WHERE is_active = 0 """ ,
        sqlite_conn_id= 'my_sqlite_conn'
    )
    
    update_values = SqliteOperator(
        task_id = 'update_values',
        sql = r""" UPDATE users SET city = 'New Jersey' WHERE name='Emmanuel' """,
        sqlite_conn_id= 'my_sqlite_conn'
    )
    
    update_values_2 = SqliteOperator(
        task_id = 'update_values_2',
        sql = r""" UPDATE users SET city = 'Lagos' WHERE name <> 'Emmanuel' """,
        sqlite_conn_id= 'my_sqlite_conn'
    )
        
    
    display_result = SqliteOperator(
        task_id = 'display_result',
        sql = r"SELECT * FROM users",
        sqlite_conn_id= 'my_sqlite_conn',
        do_xcom_push = True
    )
    
    
create_table >> [insert_values_1, insert_values_2] >> delete_values >> [update_values, update_values_2] >> display_result

