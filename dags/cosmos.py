from airflow.decorators import dag
from cosmos.providers.dbt.task_group import DbtTaskGroup
from cosmos.providers.dbt.dag import DbtDag
from pendulum import datetime

CONNECTION_ID = "dbt_snow"
DB_NAME = "ANALYTICS"
SCHEMA_NAME = "DBT_NWANKHEDE"
DBT_PROJECT_NAME = "dbt_dfs"
# the path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"
# The path to your dbt root directory
DBT_ROOT_PATH = "/opt/airflow/dags/dbt_dfs/"

PROJECT_NAME = "/opt/airflow/dags/dbt_dfs/"

example_cosmos = DbtDag(
    # dbt/cosmos-specific parameters
    dbt_root_path=DBT_ROOT_PATH,
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    dbt_project_name=DBT_PROJECT_NAME,
    conn_id=CONNECTION_ID,
#    dbt_args={"schema": "DBT_COSMOS","dbt_root_path": DBT_ROOT_PATH,"dbt_executable_path":DBT_EXECUTABLE_PATH},
#    execution_mode="virtualenv",
#    profile_args={
#       "schema":"DBT_COSMOS",
 #       "project_dir": PROJECT_NAME,
 #       "dbt_project_name":DBT_PROJECT_NAME, "conn_id":CONNECTION_ID,
 #   },
    # normal dag parameters
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_cosmos",
)
