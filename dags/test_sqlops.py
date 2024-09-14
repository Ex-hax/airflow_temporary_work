from typing import Dict, Any, Union
from datetime import datetime
from airflow.decorators import dag
from pathlib import Path
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args: Dict[str,Any]={
    'owner': 'DataPlatform',
    'depends_on_past': False,
}

DAG_ID: str = Path(__file__).stem

start_date = '{{ data_interval_start | ds }}'
asofdate = '{{ data_interval_end | ds }}'

schedule_interval = None

@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
    start_date=datetime(2024, 9, 18),
    catchup=True
)
def main() -> None:
    task_sql: SQLExecuteQueryOperator = SQLExecuteQueryOperator(
        task_id=f"""test_params_attrs""",
        conn_id="mock_db",
        sql=f"""select 1""",
        params={"a": "a"}
    )
    chain(task_sql)
main()