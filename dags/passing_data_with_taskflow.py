import json

from airflow.utils.dates import days_ago

from airflow.decorators import dag, task

from airflow.operators.python import PythonOperator

default_args ={
    'owner': 'Emmanuel',
}
    
    
@dag(
    dag_id = 'taskflow_cross_task_comms',
    description = 'taskflow API with Xcom operations',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['taskflow_api', 'xcom','python']
)
def passing_data_with_taskflow_api():
    @task
    def get_order_prices(**kwargs):
        ti = kwargs["ti"]
    
        order_price_data = {
            'o_1': 334.45,
            'o_2': 20.00,
            'o_3': 44.77,
            'o_4': 55.66,
            'o_5': 499
        }
        
        return order_price_data
    
    @task(multiple_outputs=True)
    def compute_total_and_average(order_price_data: dict):
        total = 0
        count = 0
        for _, value in order_price_data.items():
            total += value
            count += 1
            
        average = total / count
        
        return {'total': total, 'average': average}
    
    @task
    def display_result_(total, average):
    
        print("Total price of goods {total}".format(total=total))
        print("Average price of goods {average}".format(average=average))
        
    order_price_data = get_order_prices()
    total_and_average = compute_total_and_average(order_price_data)
    display_result_(total_and_average['total'], total_and_average['average'])
    
passing_data_with_taskflow_api()