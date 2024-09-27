'''
    full_dump_all -> rerun click clear 
'''
# The DAG object; we'll need this to instantiate a DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
import os, json
from pendulum import duration
from datetime import datetime
from typing import Dict, Any, Union
from pathlib import Path
import os

dir_path: str = os.path.dirname(os.path.realpath(__file__))

DAG_NAME: str = Path(__file__).stem

default_args: Dict[str, Any] = {
    'owner': 'DataPlatform',
    # 'depends_on_past': True,
    'email': ['BigdataCloudSupport@siampiwat.com','itnoc@siampiwat.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 2,
    # 'retry_delay': duration(seconds=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': True,
    # 'sla': duration(hours=2),
    # 'execution_timeout': duration(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # allback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

schedule_interval = None
@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=schedule_interval,
    concurrency=3,
    max_active_runs=1,
    start_date=datetime(2023, 2, 8),
    catchup=False,
    # owner_links={"DataPlatform": "mailto:watchara.c@siampiwat.com"},
    # access_control={
    #     "DataPlatform": ["can_read", "can_edit", "can_delete"],
    #     "DataPlatformTrainee": ["can_read", "can_edit"],
    #     "NOC": ["can_read", "can_edit"]
    # },
    tags=['sap','mssql','obs','dws','noc','qa','test']
)
def main() -> None:
    run_pymssql_task = DockerOperator(
        task_id='pyspark_connector_task',
        image='pyspark',  # Your Docker image name
        api_version='auto',
        auto_remove=True,  # Automatically remove the container after it exits
        # command='python your_script.py',  # Command to run in the container
        # docker_url='tcp://localhost:2375',  # Docker socket URL
        network_mode='host'  # Use 'bridge' or 'host' as needed
    )
    chain(run_pymssql_task)
main()