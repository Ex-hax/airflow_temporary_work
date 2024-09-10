from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

import random

def task_a_function(**kwargs):
    # Simulate a task processing logic
    # Return True or False based on some condition
    return random.choice([True, False])  # Change to False to simulate failure

def choose_branch(**kwargs):
    # Determine the next task based on the output of task_a
    ti = kwargs['ti']
    return 'task_b' if ti.xcom_pull(task_ids='task_a') else 'task_c'

def task_b_function(**kwargs):
    # Simulate task_b processing logic
    print("Executing task_b")

def task_c_function(**kwargs):
    # Simulate task_c processing logic
    print("Executing task_c")

def task_d_function(**kwargs):
    # Simulate task_d processing logic
    print("Executing task_d")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG('conditional_task_flow', default_args=default_args, schedule_interval=None) as dag:

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=task_a_function,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branch,
        provide_context=True
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=task_b_function,
        provide_context=True
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=task_c_function,
        provide_context=True
    )

    join_task = DummyOperator(
        task_id='join_task',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    task_d = PythonOperator(
        task_id='task_d',
        python_callable=task_d_function,
        #provide_context=True,
        #trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Define the task dependencies
    task_a >> branch_task
    branch_task >> task_b >> join_task
    branch_task >> task_c >> join_task
    join_task >> task_d
