import logging, json
from pathlib import Path
from typing import Any, Dict, Optional
from pendulum import duration, datetime

from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import BaseOperator, chain
from airflow.models.dagrun import DagRun
from airflow.models.variable import Variable
from airflow.datasets import Dataset
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.context import Context

# from siampiwat.common.repositories import OBSRepository
# from siampiwat.common.utils import load_json_config, schema_mapping
# from siampiwat.common.logging.get_logger import Logger

# logger = Logger.get_logger()


DAG_OWNER = "DataPlatform"
DAG_ID = Path(__file__).stem
SCHEDULE = None
ACCESS_CONTROLS = {
    DAG_OWNER: ["can_read", "can_edit", "can_delete"],
    DAG_OWNER + "Trainee": ["can_read", "can_edit"],
    "NOC": ["can_read", "can_edit"],
}
TIMEZONE = "UTC"
TAGS = ["transform", "domain:retail", "source:ax_spw_stagingdb", "noc", "priority:high", "author:watchara.c@siampiwat.com"]
DAG_OWNER_EMAIL = "BigdataCloudSupport@siampiwat.com"
NOC_EMAIL = "itnoc@siampiwat.com"

DEFAUTL_ARGS = {
    "owner": DAG_OWNER,
    "email": [DAG_OWNER_EMAIL, NOC_EMAIL], # List of email addresses to notify on failure or retry
    "email_on_failure": False, # Change to True to send email on failure
    "email_on_retry": False, # Change to True to send email on retry
    "retries": 3, # Change to 0 to disable retries
    "retry_delay": duration(seconds=300), # Change to duration(seconds=300) to 5 minutes
    "depends_on_past": False, # Change to True to run the DAG on the previous execution date
    "wait_for_downstream": False, # Change to True to wait for downstream tasks to complete
    "sla": duration(hours=2), # Change to duration(hours=2) to 2 hours for SLA
    "execution_timeout": duration(hours=2), # Change to duration(hours=2) to 2 hours for execution timeout
}


@task()
def transform_landing_to_staging(x, dag_run: DagRun) -> None:
    print(x)
    ...


@task_group
def landing_to_staging(**context: Context) -> Optional[BaseOperator]:
    """
    Transforms data from the landing table to the staging table.

    Args:
    -----
    :conf (Dict[str, Any]): The configuration dictionary or Airflow variable.
    
    Returns:
    --------
    Optional[BaseOperator]: The last task in the task group.
    """
    x=context
    transform_landing_to_staging_op = transform_landing_to_staging(x)

    chain(transform_landing_to_staging_op)


@dag(
    dag_id=DAG_ID,
    default_args=DEFAUTL_ARGS,
    # owner_links={DAG_OWNER: f"mailto:{DAG_OWNER_EMAIL}"},
    # access_control=ACCESS_CONTROLS,
    schedule=SCHEDULE,
    start_date=datetime(2024, 8, 5, 17, 0, 0, tz=TIMEZONE),
    concurrency=5, # Allow 5 concurrent tasks run simultaneously at the same time
    max_active_runs=1, # Only allow 1 active run at the same time
    catchup=False, # Do not catchup to the past
    # template_searchpath=["/opt/airflow/dags/data_engineer/ax_spw/include/sql"],
    tags=TAGS,
)
def ax_spw_stagingdb_transform_dag() -> None:
    landing_to_staging_op = landing_to_staging()
    chain(landing_to_staging_op)
dag = ax_spw_stagingdb_transform_dag()