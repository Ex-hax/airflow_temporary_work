from typing import Dict, Any, Union
from datetime import datetime
from airflow.decorators import dag, task
from pathlib import Path
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.variable import Variable

default_args: Dict[str,Any]={
    'owner': 'DataPlatform',
    'depends_on_past': False,
}

DAG_ID: str = Path(__file__).stem

# start_date = '{{ data_interval_start | ds }}'
# asofdate = '{{ data_interval_end | ds }}'

schedule_interval = '*/1 * * * *'

@task
def test(logical_datetime, **context):
    print(logical_datetime)

@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
    start_date=datetime(2024, 9, 20),
    catchup=True
)
def main() -> None:
    
    var = Variable.get('test', deserialize_json=True)
    test_task_op: Task = test('{{logical_date}}')
    chain(test_task_op)
    
main()