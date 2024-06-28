import csv
from pathlib import Path

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Emmanuel'
}

WORK_DIR = Path('/home/endie/Projects/Data_Engineering/airflow_prject_3')

def saving_to_csv_(ti):
    filter_data = ti.xcom_pull(task_ids='filtering_customers')
    file_path = WORK_DIR.joinpath('output', 'filtered_customer_data.csv')
    with open(file_path, 'w', newline='') as file:

        writer = csv.writer(file)

        writer.writerow(['Name', 'Product', 'Price'])

        for row in filter_data:
            writer.writerow(row)

with DAG(
    dag_id = 'postgres_pipeline',
    description = 'Pipeline using postgres operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags = ['postgres', 'python', 'pipeline'],
    template_searchpath = '/home/endie/Projects/Data_Engineering/airflow_prject_3/sql_statements'
) as dag:

    create_table_customers = PostgresOperator(
        task_id = 'create_table_customers',
        postgres_conn_id = 'postgres_conn',
        sql = 'create_table_customers.sql'
    )

    create_table_customer_purchases = PostgresOperator(
        task_id = 'create_table_customer_purchases',
        postgres_conn_id = 'postgres_conn',
        sql = 'create_table_customer_purchases.sql'
    )

    insert_customers = PostgresOperator(
        task_id = 'insert_customers',
        postgres_conn_id = 'postgres_conn',
        sql = 'insert_customers.sql'
    )

    insert_customer_purchases = PostgresOperator(
        task_id = 'insert_customer_purchases',
        postgres_conn_id = 'postgres_conn',
        sql = 'insert_customer_purchases.sql'
    )

    joining_table = PostgresOperator(
        task_id = 'joining_table',
        postgres_conn_id = 'postgres_conn',
        sql = 'joining_table.sql'
    )

    filtering_customers = PostgresOperator(
        task_id = 'filtering_customers',
        postgres_conn_id = 'postgres_conn',
        sql = '''
            SELECT name, product, price
            FROM complete_customer_details
            WHERE price BETWEEN %(lower_bound)s AND %(upper_bound)s
        ''',
        parameters = {'lower_bound': 5, 'upper_bound': 9}
    )
    
    saving_to_csv = PythonOperator(
        task_id='saving_to_csv',
        python_callable=saving_to_csv_
    )

    create_table_customers >> create_table_customer_purchases >> \
        insert_customers >> insert_customer_purchases >> \
        joining_table >> filtering_customers >> saving_to_csv

