import re
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow_options.load_variables import load_env_constants_sourcing
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import (
    GoogleCloudStorageDeleteOperator,
)
from operators.GenericDataWarehouseOperator import GenericDataWarehouseOperator
from airflow_options.constants import dag_options
from airflow_options import utils
from airflow_options import vm_start_stop
from airflow.utils.task_group import TaskGroup
from tasks import connection as conn

from airflow_options import slack_integration as sl

from dataflow_options.utils import Logger
import re

log = Logger(__name__)

dag_options = {
    "owner": dag_options.owner,
    "retries": dag_options.retries,
    "retry_delay": dag_options.retry_delay,
    "email_on_failure": dag_options.email_on_failure,
    "email_on_retry": dag_options.email_on_retry,
    # "project_id": dag_options.project_id,
    # "on_failure_callback":sl.slack__on_task_failure,
    #'on_success_callback':sn.on_task_success,
    # 'on_failure_callback':sn.on_task_failure,
    # "provide_context":True,
}

parent_dag = "sourcing_intermediate_queries"

overall_config = utils.get_overall_config()
sourcing_config = overall_config["sourcing_config"]
TENANT = sourcing_config.get("tenant")
PIPELINE = sourcing_config.get("pipeline")
billing_project_id = sourcing_config.get("billing_project_id")

unique_entities = overall_config["unique_entities"]
unique_views = overall_config["unique_views"]


with DAG(
    dag_id=parent_dag,
    default_args=dag_options,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    max_active_tasks=int(sourcing_config["dag_configs"]["max_active_tasks"]),
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    concurrency=int(sourcing_config["dag_configs"]["dag_concurrency"]),
) as dag:

    load_env_vars_intermediate = PythonOperator(
        task_id="load_env_variables_intermediate",
        python_callable=load_env_constants_sourcing,
    )


    add_warehouse_connection_python = PythonOperator(
        task_id="add_warehouse_connection_python",
        python_callable=conn.add_warehouse_connection,
        provide_context=True,
    )

    get_intermediate_queries = PythonOperator(
        task_id="get_the_intermediate_queries_from_gcs",
        python_callable=utils.get_query_from_gcs,
    )

    # intermediate_queries_start = PythonOperator(
    #    task_id="start_intermediate_queries",
    #    python_callable=validate_start_of_intermediate_queries,
    # )

    # fetch the sourcing parameters from mapping table
    fetch_the_sourcing_parameters = PythonOperator(
        task_id="fetch_the_sourcing_params_for_intermediate_queries",
        python_callable=utils.fetch_param,
    )

    (
        load_env_vars_intermediate
        >> add_warehouse_connection_python
        >> fetch_the_sourcing_parameters
        >> get_intermediate_queries
        # >> intermediate_queries_start
    )

    # run intermediate queries
    for entity in unique_entities:

        with TaskGroup(group_id=f"{entity}_intermediate") as tg:

            create_intermediate_table = GenericDataWarehouseOperator(
                task_id=f"create_{entity}_intermediate_table",
                sql=f"{{{{var.value.get('{entity}_intermediate_query', '')}}}}",
                conn_id="warehouse_connection",
                warehouse="""{{ var.value.get("warehouse") }}""",
                # extras = """{{var.value.get("warehouse_kwargs")}}"""
            )

            update_filter_param_in_mapping = PythonOperator(
                task_id=f"update_filter_param_{entity}",
                python_callable=utils.update_filter_param,
                op_kwargs={"entity": entity},
            )

            create_intermediate_table >> update_filter_param_in_mapping

        # intermediate_queries_start >> tg
        # intermediate_queries_start = tg
        get_intermediate_queries >> tg
        get_intermediate_queries = tg

    intermediate_queries_end = DummyOperator(task_id="end_intermediate_queries")
    # intermediate_queries_start >> intermediate_queries_end
    get_intermediate_queries >> intermediate_queries_end

    # clean the csv files from buckets
    clean_buckets = []
    for view in unique_views:
        clean_bucket = GoogleCloudStorageDeleteOperator(
            task_id=f"clean_the_bucket_{re.sub('[^A-Za-z0-9_]', '_', view)}",
            bucket_name="{{var.value.get('gcp_bucket')}}",
            prefix=f"/{view}/",
            gcp_conn_id="google_cloud_default",
            trigger_rule="all_done",
        )
        clean_buckets.append(clean_bucket)

    # start the ingestion vm
    ingestion_vm_start = PythonOperator(
        task_id="ingestion_vm_start",
        python_callable=vm_start_stop.start_vm_instance,
        trigger_rule="all_success",
    )

    # trigger data ingestion dag
    trigger_data_ingestion = PythonOperator(
        task_id="trigger_data_ingestion",
        python_callable=utils.trigger_dag,
        op_kwargs={
            "web_server_url": "{{var.value.get('data_ingestion_pipeline_url', '')}}",
            "dag_id": "{{var.value.get('data_ingestion_dag_id', '')}}",
            "data": {
                "execution_date": "2021-01-01T15:00:00Z",
                "conf": {},
            },
        },
        on_success_callback=sl.slack__last_task_success,
        trigger_rule="all_success",
    )

    cleanup_task = BashOperator(
        task_id="cleanup_old_logs",
        trigger_rule="all_done",
        bash_command="./cleanup_old_logs.sh",
        dag=dag,
    )

    slack_notification_task = PythonOperator(
        task_id="slack_notification_task",
        trigger_rule="all_done",
        python_callable=sl.slack__on_task_failure,
        provide_context=True,
    )

    (
        intermediate_queries_end
        >> ingestion_vm_start
        >> trigger_data_ingestion
        >> clean_buckets
        >> cleanup_task
        >> slack_notification_task
    )
