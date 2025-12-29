from airflow import DAG
import pendulum
from airflow_options.load_variables import load_env_constants_sourcing
from airflow.operators.python_operator import PythonOperator


with DAG(
    dag_id="load_env_vars_sourcing",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
) as dag:

    load_env_vars_sourcing = PythonOperator(
        task_id="load_env_variables", python_callable=load_env_constants_sourcing
    )


