import json

from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args ={
    'owner': 'Emmanuel',
}

def get_order_prices_(**kwargs):
    ti = kwargs["ti"]
    
    order_price_data = {
        'o_1': 334.45,
        'o_2': 20.00,
        'o_3': 44.77,
        'o_4': 55.66,
        'o_5': 499
    }
    
    order_price_string = json.dumps(order_price_data)
    
    ti.xcom_push('order_price_data', order_price_string)
    
    
def compute_sum_(**kwargs):
    ti = kwargs["ti"]
    
    order_price_string = ti.xcom_pull(task_ids='get_order_prices', key='order_price_data')
    
    print(order_price_string)
    
    order_price_data = json.loads(order_price_string)
    
    total = 0
    for _, value in order_price_data.items():
        total += value
        
    ti.xcom_push('total_price', total)
    
    
def compute_average_(**kwargs):
    ti = kwargs["ti"]
    
    order_price_string = ti.xcom_pull(task_ids='get_order_prices', key='order_price_data')
    
    print(order_price_string)
    
    order_price_data = json.loads(order_price_string)
    
    total = 0
    count = 0
    for _, value in order_price_data.items():
        total += value
        count += 1
        
    average = total / count
    
    ti.xcom_push('average_price', average)
    
    
def display_result_(**kwargs):
    ti = kwargs["ti"]
    
    total = ti.xcom_pull(task_ids = 'compute_sum', key='total_price')
    average = ti.xcom_pull(task_ids = 'compute_average', key='average_price')
    
    print("Total price of goods {total}".format(total=total))
    print("Average price of goods {average}".format(average=average))
    
    
with DAG(
    dag_id = 'cross_task_comms_ops',
    description = 'Xcom with operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['xcom','python', 'operators']
) as dag:
    get_order_prices = PythonOperator(
        task_id = 'get_order_prices',
        python_callable = get_order_prices_
    )
    
    compute_sum = PythonOperator(
        task_id = 'compute_sum',
        python_callable = compute_sum_
    )
    
    compute_average = PythonOperator(
        task_id = 'compute_average',
        python_callable = compute_average_
    )
    
    display_result = PythonOperator(
        task_id = 'display_result',
        python_callable = display_result_
    )
    
get_order_prices >> [compute_sum, compute_average] >> display_result