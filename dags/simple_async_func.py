from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from operators__c.async_python_operator__c import AsyncPythonOperator__c, AsyncPythonOperatorTask__c as task_async

# Define an async function to use with the operator
async def async_function_a(*args: list, **kwargs: dict) -> None:
    # Example transformation for args (you should define your specific transformation logic)
    transformed_args: list = list(map(lambda x: x.upper(), args))
    print("Transformed Arguments:", transformed_args)

    # Example transformation for kwargs (ensure values are strings or handle appropriately)
    transformed_kwargs: dict = {key: value.upper() if isinstance(value, str) else value for key, value in kwargs.items()}
    print("Transformed Keyword Arguments:", transformed_kwargs)

    context = get_current_context()
    context['ti'].xcom_push(key='transformed_data', value={'args': transformed_args, 'kwargs': transformed_kwargs})

async def async_function_b() -> None:
    # Pull data from XCom
    context = get_current_context()
    data = context['ti'].xcom_pull(task_ids='async_task_a', key='transformed_data')
    
    if data:
        transformed_args: list = data['args']
        transformed_kwargs: dict = data['kwargs']
        print("Retrieved Transformed Arguments:", transformed_args)
        print("Retrieved Transformed Keyword Arguments:", transformed_kwargs)

@task_async.async_task
async def async_function_c() -> None:
    # Pull data from XCom
    print('Can use @task with async task')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'catch_up': False,
    'depends_on_past': True
}

@dag(
    'async_operator_dag', default_args=default_args, schedule_interval=None
)
def main() -> None:
    async_task_a = AsyncPythonOperator__c(
        task_id='async_task_a',
        python_callable=async_function_a,
        op_args=['arg1', 'arg2'],  # Example of passing args to the function
        op_kwargs={'a': 'a', 'b': 'b'}  # Example of passing kwargs to the function
    )

    async_task_b = AsyncPythonOperator__c(
        task_id='async_task_b',
        python_callable=async_function_b,
    )

    async_function_c_task = async_function_c()

    chain(async_task_a , async_task_b, async_function_c_task)
main()
