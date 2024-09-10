from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import uuid
from airflow.operators.python_operator import PythonOperator
from airflow.utils.context import Context

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

@task
def ctx(data, **context: Context):
    print(data.keys())
    print(context.keys())

def task_a_function(connn: str , sch: str, ao: str='??', bo: str='!!' ,**context: Context):
    print(connn, sch)
    print(ao, bo)
    print(context.get('b'))
    #print(kwargs.keys())

with DAG(
    'contexts',
    default_args=default_args,
    description='A simple kwargs',
    schedule_interval=None,
    catchup=False,
) as dag:
    ctx = ctx({'a': 1})
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=task_a_function,
        #op_args=['connn', 'sch'],
        op_kwargs={'b': 2, 'connn': 'connn', 'sch': 'sch', 'bo': 'xxxx'},
        provide_context=True
    )

    ctx >> task_a