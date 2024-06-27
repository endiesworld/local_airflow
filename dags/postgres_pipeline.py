from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Emmanuel'
}


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
        sql= 'create_table_customers.sql',
        postgres_conn_id= 'postgres_conn',
    )
    
    create_table_customer_purchases = PostgresOperator(
        task_id = 'create_table_customer_purchase',
        sql= 'create_table_customer_purchases.sql',
        postgres_conn_id= 'postgres_conn',
    )
    
    
create_table_customers >> create_table_customer_purchases

