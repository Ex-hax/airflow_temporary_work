from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import uuid
from airflow.operators.python_operator import PythonOperator
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
import json
import requests as r

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

@task
def ctx(a, **context):
    print(a)
    print(context)

def task_a_function(connn: str , sch: str, ao: str='??', bo: str='!!' ,**context: Context):
    print(connn, sch)
    print(ao, bo)
    print(context.get('b'))
    print('>>>>>>>>>>>>>>>>')
    print(context.get('start_date'))
    print(context.get('asofdate'))
    print(context.get('logical_date'))
    #print(kwargs.keys())

with DAG(
    'contexts',
    default_args=default_args,
    description='A simple kwargs',
    schedule_interval=None,
    catchup=False,
) as dag:
    _ctx = {
        'start_date': '{{ data_interval_start | ds }}',
        'asofdate': '{{ data_interval_end | ds }}'
    }
    ctx_task = ctx({'a': 1})
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=task_a_function,
        #op_args=['connn', 'sch'],
        op_kwargs={
            'b': 2,
            'connn': 'connn',
            'sch': 'sch',
            'bo': 'xxxx',
            'start_date': _ctx['start_date'],
            'asofdate': _ctx['asofdate'],
            'logical_date': '{{logical_date}}'
        },
        provide_context=True
    )

    chain(ctx_task, task_a)