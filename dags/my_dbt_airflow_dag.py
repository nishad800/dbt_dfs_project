from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

def run_dbt():
    # Replace <container_url> and <path/to/your/profiles.yml> with the actual container URL and profiles.yml path
    dbt_command = "dbt run --profiles-dir https://dbtfile2023.blob.core.windows.net/dbtcontainer/dags/dbt_dfs/profiles.yml --profile dbt_dfs"
    subprocess.run(dbt_command, shell=True)

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Set your desired start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'run_dbt_dag',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval or None if manual execution
)

run_dbt_task = PythonOperator(
    task_id='run_dbt_task',
    python_callable=run_dbt,
    dag=dag,
)

# You can add other tasks and dependencies as needed

if __name__ == "__main__":
    dag.cli()
