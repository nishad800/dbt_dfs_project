
from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

from airflow.decorators import dag

# then, in your DAG
with DAG(
    dag_id="extract_dag",
    start_date=datetime(2023, 09, 20),
    schedule="none",
):
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig("dbt_demo"),
        conn_id="dbt_demo",
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg >> e2