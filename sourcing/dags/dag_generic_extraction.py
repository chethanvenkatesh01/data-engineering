# try:
import re
import pendulum
import re
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from airflow_options import utils
from airflow_options.constants import (
    dag_options,
    DB_TO_BQ_JAR,
    GCS_TO_BQ_JAR,
    SF_TO_BQ_JAR,
)

from airflow_options.dag_triggers import DagTrigger
from airflow_custom_operators.db_to_warehouse import DbToWarehouseOperator
from airflow_custom_operators.gcs_to_warehouse import GcsToWarehouseOperator
from airflow_custom_operators.sftp_to_warehouse import SftpToWarehouseOperator
from airflow_custom_operators.snowflake_to_bq_branch import SnowflakeToBQPipeline
from airflow_custom_operators.notification_operator import SendNotificationMailOperator
from airflow_custom_operators.validate_intermediate import ValidateIntermediateOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow_options.row_count_validation import RowCountValidation
from airflow_options.slack_integration import (
    slack__first_task_success,
    slack__on_task_failure,
)
from operators.GenericDataWarehouseOperator import GenericDataWarehouseOperator
from operators.utils import set_warehouse_kwargs


# def add_gcp_conn():
#     from airflow.models import Variable
#     from airflow.models import Connection
#     from json import dumps as json_dumps
#     from airflow import settings
#     source_project_conn = Connection(
#         conn_id="google_cloud_default",
#         conn_type="gcpbigquery",
#         extra=json_dumps(
#             eval(
#                 """{{
#             "extra__google_cloud_platform__project":"{project}"
#             }}""".format(
#                     project=Variable.get("billing_project_id")
#                 )
#             )
#         ),
#     )
#     session = settings.Session()
#     if not (
#         session.query(Connection)
#         .filter(Connection.conn_id == source_project_conn.conn_id)
#         .first()
#     ):
#         session.add(source_project_conn)
#         session.commit()


def add_gcp_connection():
    from airflow.models import Connection, Variable
    import json
    from airflow import settings

    """ Add Airflow connections for GCP default and billing projects """

    # Define OAuth scopes
    scopes = [
        "https://www.googleapis.com/auth/pubsub",
        "https://www.googleapis.com/auth/datastore",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write",
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    # Define connection details
    connections = [
        {
            "conn_id": "google_cloud_default",  # Default connection
            "project_var": "gcp_project",  # Airflow variable for the default project
        },
        {
            "conn_id": "warehouse_connection",  # Billing project connection
            "project_var": "billing_project_id",  # Airflow variable for the billing project
        },
    ]

    # Initialize Airflow session
    session = settings.Session()

    for conn_details in connections:
        conn_id = conn_details["conn_id"]
        project_var = conn_details["project_var"]

        # Get project ID from Airflow variable
        project_id = Variable.get(project_var, default_var=None)
        if not project_id:
            raise ValueError(
                f"Project ID for {project_var} is not set in Airflow Variables"
            )

        # Create Connection object
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="google_cloud_platform",
        )
        conn_extra = {
            "extra__google_cloud_platform__scope": ",".join(scopes),
            "extra__google_cloud_platform__project": project_id,
        }
        conn_extra_json = json.dumps(conn_extra)
        new_conn.set_extra(conn_extra_json)

        # Check if connection already exists
        existing_conn = (
            session.query(Connection)
            .filter(Connection.conn_id == new_conn.conn_id)
            .first()
        )
        if existing_conn:
            session.delete(existing_conn)
            session.commit()
        session.add(new_conn)
        session.commit()
        print(f"GCP connection with `conn_id`={conn_id} created successfully.")


def add_snowflake_connection():
    from airflow.models import Connection, Variable
    from airflow import settings
    import json

    """Add an Airflow connection for Snowflake."""
    new_conn = Connection(
        conn_id="warehouse_connection",
        conn_type="snowflake",
        host=Variable.get("sf_host"),
        schema=Variable.get("sf_schema"),
        login=Variable.get("sf_user"),
        password=Variable.get("sf_password"),
    )

    conn_extra = {
        "account": Variable.get("sf_account"),
        "warehouse": Variable.get("sf_warehouse"),
        "database": Variable.get("sf_database"),
        "role": Variable.get("sf_role"),
    }
    conn_extra_json = json.dumps(conn_extra)
    new_conn.set_extra(conn_extra_json)

    # Save connection to Airflow
    session = settings.Session()
    if (
        not session.query(Connection)
        .filter(Connection.conn_id == new_conn.conn_id)
        .first()
    ):
        session.add(new_conn)
        session.commit()
        print(f"Connection with conn_id='{new_conn.conn_id}' added successfully.")
    else:
        print(f"A connection with conn_id='{new_conn.conn_id}' already exists.")


def add_psg_connection():
    from airflow import models
    from airflow.models import Connection
    import urllib
    from airflow import settings

    """ Add a airflow connection for PSG """
    curr_conn_id = models.Variable.get(
        "postgres_conn_id", default_var="postgres_default"
    )
    # new_conn_id = "postgres_default" + str(int(time()))
    new_conn_id = "postgres_default"
    new_conn = Connection(
        conn_id=new_conn_id,
        conn_type="postgres",
        host=models.Variable.get("PGXHOST"),
        login=models.Variable.get("PGXUSER"),
        port=models.Variable.get("PGXPORT"),
        schema=models.Variable.get("PGXDATABASE"),
        password=urllib.parse.quote(models.Variable.get("PGXPASSWORD")),
    )

    session = settings.Session()
    curr_conn_obj = session.query(Connection).filter(Connection.conn_id == curr_conn_id)
    if curr_conn_obj.first():
        session.delete(curr_conn_obj.first())
        session.commit()
    session.add(new_conn)
    session.commit()
    models.Variable.set("postgres_conn_id", new_conn_id)


def add_warehouse_connection():
    from airflow.models import Variable

    if Variable.get("warehouse") == "SNOWFLAKE":
        add_snowflake_connection()
    else:
        add_gcp_connection()



def validate_trigger(view: str):
    from airflow.models import Variable

    if Variable.get(f"{view}_param_connector").upper() in [
        "SFTP",
        "SNOWFLAKE",
        "GCS",
    ]:
        pass
    view_trigger_status_mapping = eval(Variable.get("view_trigger_status_mapping"))
    view_trigger_mandatory_mapping = eval(
        Variable.get("view_trigger_mandatory_mapping", default_var="{}")
    )
    if (view in view_trigger_status_mapping and view_trigger_status_mapping[view]) or (
        not view_trigger_mandatory_mapping.get(view, False)
    ):
        return True
    else:
        raise Exception(f"Trigger constraints failed for {view}")


def get_trigger_timeout_seconds():
    from airflow.models import Variable
    from datetime import datetime
    import pytz

    timezone = pytz.timezone(sourcing_config.get("dag_configs")["dag_timezone"])
    curr_datetime = datetime.now(timezone)
    curr_date = curr_datetime.date()
    trigger_cutoff_time = datetime.strptime(
        sourcing_config.get("dag_configs").get("trigger_cutoff_time"), "%H:%M"
    ).time()
    trigger_cutoff_datetime = timezone.localize(
        datetime.combine(curr_date, trigger_cutoff_time)
    )
    Variable.set(
        "cutoff_time_left_in_seconds", (trigger_cutoff_datetime - curr_datetime).seconds
    )
    Variable.set("internal_cutoff_curr_time", str(curr_datetime))
    Variable.set("internal_cutoff_time", str(trigger_cutoff_datetime))
    if curr_datetime >= trigger_cutoff_datetime:
        return 0
    else:
        return (trigger_cutoff_datetime - curr_datetime).seconds


def decide_branch_for_extraction():
    from airflow.models import Variable

    extraction_flag = eval(Variable.get("extraction_flag", default_var="{}"))

    if extraction_flag in (True, "True"):
        return ["fetch_the_sourcing_params"]
    else:
        return decide_trigger_intermediate_dag()


def decide_trigger_intermediate_dag():
    from airflow.models import Variable

    intermediate_queries_flag = eval(
        Variable.get("intermediate_queries_flag", default_var="{}")
    )

    if intermediate_queries_flag in (True, "True"):
        return ["validate_start_of_intermediate_queries"]
    else:
        return ["end_dag"]


def trigger_remote_scheduling_dag():
    import logging
    from airflow.models import Variable
    import requests
    from requests.auth import HTTPBasicAuth

    log = logging.getLogger(__name__)
    # don't define the env_url in .env for same env. As it is being triggered by above
    # default_url = "None"
    base_urls = eval(
        Variable.get("remote_urls_for_sourcing_dags", default_var="[]")
    )  # [base_url_prod, base_url_dev, base_url_test, base_url_uat]
    if not base_urls:
        return
    log.info(f"base_urls {str(base_urls)}")
    dag_id = "sourcing_intermediate_queries"
    auth = HTTPBasicAuth("airflow", "airflow")
    # Set the endpoint for triggering a DAG run
    endpoint = f"/api/v1/dags/{dag_id}/dagRuns"

    for base_url in base_urls:
        # if base_url != "None":
        log.info(f"Hitting for {base_url}")
        # Make the HTTP POST request to trigger the DAG run
        response = requests.post(f"{base_url}{endpoint}", auth=auth, json={})
        log.info(f"response : {response.json()}")
        # Check the response status code
        if response.status_code == 200:
            print(f"Successfully triggered DAG run for DAG '{dag_id}'")
            log.info(f"Successfully triggered DAG run for DAG '{dag_id}'")
        else:
            print(f"Failed to trigger DAG run: {response.text}")
            log.info(f"Failed to trigger DAG run: {response.text}")


def get_file_suffix_from_config(config: dict):
    from pathlib import Path

    # file_pattern:str = config.get("file_pattern", "yyyy-MM-dd/{view}.*.csv")
    file_pattern: str = config.get("file_pattern", "yyyyMMdd/{view}/*.parquet")
    return Path(file_pattern).suffix.lstrip(".")


def get_source_config(view: str, **kwargs):
    from airflow.models import Variable
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    source_config = Variable.get(f"{view}_param_source_config")
    name = f"projects/{Variable.get('gcp_project')}/secrets/{source_config}/versions/latest"
    response = client.access_secret_version(name=name)
    source_config = eval(response.payload.data.decode("UTF-8"))
    Variable.set(f"source_config_{view}", source_config)
    print(kwargs)
    kwargs["ti"].xcom_push(key=f"return_value", value=source_config)
    return source_config


def load_environment_variables():
    from airflow_options.load_variables import load_env_constants_sourcing

    load_env_constants_sourcing()


# initialization
row_count_validation_obj = RowCountValidation()
# dag_configs = eval(Variable.get('dag_configs', default_var={}))
overall_config = utils.get_overall_config()
sourcing_config = overall_config["sourcing_config"]
TENANT = sourcing_config.get("tenant")
PIPELINE = sourcing_config.get("pipeline")

dag_options = {
    "owner": dag_options.owner,
    "retries": dag_options.retries,
    "retry_delay": dag_options.retry_delay,
    "email_on_failure": dag_options.email_on_failure,
    "email_on_retry": dag_options.email_on_retry,
}


with DAG(
    dag_id="sourcing_extraction",
    default_args=dag_options,
    schedule=sourcing_config.get("dag_configs", {}).get("next_cron", None),
    start_date=pendulum.datetime(
        2021, 1, 1, tz=sourcing_config.get("dag_configs", {}).get("dag_timezone")
    ),
    max_active_tasks=int(
        sourcing_config.get("dag_configs", {}).get("max_active_tasks", "32")
    ),
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    concurrency=int(
        sourcing_config.get("dag_configs", {}).get("dag_concurrency", "32")
    ),
) as dag:

    # starting point
    sourcing_start = DummyOperator(
        task_id="sourcing",
        trigger_rule="all_success",
        on_success_callback=slack__first_task_success,
    )


    load_env_vars_sourcing = PythonOperator(
        task_id="load_env_variables_extraction",
        python_callable=load_environment_variables,
    )

    add_warehouse_connection = PythonOperator(
        task_id="add_warehouse_conn", trigger_rule="all_success", python_callable=add_warehouse_connection
    )

    trigger = DagTrigger(
        trigger_type=sourcing_config.get("dag_configs", {}).get("trigger_type"),
        poke_interval=300,
        timeout=int(
            sourcing_config.get("dag_configs", {}).get("trigger_timeout", 3600)
        ),
        mode="reschedule",
        trigger_mapping_table=sourcing_config.get("dag_configs", {}).get(
            "trigger_mapping_table"
        ),
    ).getTrigger()

    # fetch the sourcing parameters from mapping table
    fetch_the_sourcing_parameters = PythonOperator(
        task_id="fetch_the_sourcing_params",
        python_callable=utils.fetch_param,
        trigger_rule="all_success",
    )

    send_notification_mail = SendNotificationMailOperator(
        task_id="send_notification_mail_op",
        trigger_type="{{ var.value.get('trigger_type') }}",
    )

    view_load_tasks = []
    for view in overall_config["unique_views"]:
        normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', view)
        with TaskGroup(group_id=f"source_{normalized_view_name}") as tg:
            start_task = PythonOperator(
                task_id=f"validate_{normalized_view_name}_trigger",
                python_callable=validate_trigger,
                op_kwargs={"view": view},
                trigger_rule="all_done",
            )
            # get the config from secret manager
            get_source_config_view = PythonOperator(
                task_id=f"get_source_config_{normalized_view_name}",
                python_callable=get_source_config,
                op_kwargs={"view": view},
                provide_context=True,
                trigger_rule="all_success",
            )
            sftp_to_warehouse = SftpToWarehouseOperator(
                            task_id=f"sftp_pull_{normalized_view_name}_to_warehouse",
                            xcom_task_id=f"source_{normalized_view_name}.get_source_config_{normalized_view_name}",
                            view=view,
                            runner="DataflowRunner",
                            tenant=TENANT,
                            pipeline=PIPELINE,
                            project="{{ var.value.get('gcp_project') }}",
                            service_account="{{ var.value.get('service_account_email') }}",
                            region="{{ var.value.get('region') }}",
                            temp_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/temp/' }}",
                            staging_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/staging/' }}",
                            num_workers=5,
                            max_num_workers=5,
                            worker_machine_type="n2-standard-8",
                            subnetwork="{{ var.value.get('subnetwork') }}",
                            use_public_ips=False,
                            default_file_encoding="{{ var.value.get('default_file_encoding') }}",
                            header = True
                        )

            # sftp_to_bq = SftpToBQPipeline(
            #     task_id=f"sftp_pull_{normalized_view_name}_to_warehouse",
            #     xcom_task_id=f"source_{normalized_view_name}.get_source_config_{normalized_view_name}",
            #     view=view,
            #     runner="DataflowRunner",
            #     jar=SFTP_TO_BQ_JAR,
            #     tenant=TENANT,
            #     pipeline=PIPELINE,
            #     project="{{ var.value.get('gcp_project') }}",
            #     billing_project="{{ var.value.get('billing_project_id') }}",
            #     service_account="{{ var.value.get('service_account_email') }}",
            #     region="{{ var.value.get('region') }}",
            #     bigquery_region="{{ var.value.get('bigquery_region', 'US') }}",
            #     temp_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/temp/' }}",
            #     staging_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/staging/' }}",
            #     num_workers=5,
            #     max_num_workers=5,
            #     worker_machine_type="n2-standard-8",
            #     subnetwork="{{ var.value.get('subnetwork') }}",
            #     use_public_ips=False,
            #     bigquery_project="{{ var.value.get('gcp_project') }}",
            #     bigquery_dataset="{{ var.value.get('gcp_dataset') }}",
            #     bigquery_table=normalized_view_name,
            #     header=True,
            #     default_file_encoding="{{ var.value.get('default_file_encoding') }}",
            # )
            db_to_warehouse = DbToWarehouseOperator(
                            task_id=f"db_pull_{normalized_view_name}_to_warehouse",
                            xcom_task_id=f"source_{normalized_view_name}.get_source_config_{normalized_view_name}",
                            view=view,
                            runner="DataflowRunner",
                            tenant=TENANT,
                            pipeline=PIPELINE,
                            project="{{ var.value.get('gcp_project') }}",
                            service_account="{{ var.value.get('service_account_email') }}",
                            region="{{ var.value.get('region') }}",
                            temp_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/temp/' }}",
                            staging_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/staging/' }}",
                            num_workers=5,
                            max_num_workers=5,
                            worker_machine_type="n2-standard-8",
                            subnetwork="{{ var.value.get('subnetwork') }}",
                            use_public_ips=False,
                            db_table=view,
                            query_partitioning_threshold=1000000000
                        )

            # sql_gcs_to_warehouse = DbToBQPipeline(
            #     task_id=f"sql_pull_{normalized_view_name}_to_warehouse",
            #     xcom_task_id=f"source_{normalized_view_name}.get_source_config_{normalized_view_name}",
            #     view=view,
            #     runner="DataflowRunner",
            #     jar=DB_TO_BQ_JAR,
            #     tenant=TENANT,
            #     pipeline=PIPELINE,
            #     project="{{ var.value.get('gcp_project') }}",
            #     billing_project="{{ var.value.get('billing_project_id') }}",
            #     service_account="{{ var.value.get('service_account_email') }}",
            #     region="{{ var.value.get('region') }}",
            #     bigquery_region="{{ var.value.get('bigquery_region', 'US') }}",
            #     temp_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/temp/' }}",
            #     staging_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/staging/' }}",
            #     num_workers=5,
            #     max_num_workers=5,
            #     worker_machine_type="n2-standard-8",
            #     subnetwork="{{ var.value.get('subnetwork') }}",
            #     use_public_ips=False,
            #     db_table=view,
            #     bigquery_dataset="{{ var.value.get('gcp_dataset') }}",
            #     bigquery_table=normalized_view_name,
            #     query_partitioning_threshold=1000000000,
            # )
            gcs_to_warehouse_task = GcsToWarehouseOperator(
                task_id=f"gcs_pull_{normalized_view_name}_to_warehouse",
                xcom_task_id=f"source_{normalized_view_name}.get_source_config_{normalized_view_name}",
                view=view,
                runner="DataflowRunner",
                tenant=TENANT,
                pipeline=PIPELINE,
                project="{{ var.value.get('gcp_project') }}",
                service_account="{{ var.value.get('service_account_email') }}",
                region="{{ var.value.get('region') }}",
                gcp_bucket="{{ var.value.get('gcp_bucket', 'sourcing_ingestion') }}",
                num_workers=5,
                max_num_workers=5,
                worker_machine_type="n2-standard-8",
                subnetwork="{{ var.value.get('subnetwork') }}",
                use_public_ips=False,
                default_file_encoding="{{ var.value.get('default_file_encoding') }}",
            )

            snowflake_to_warehouse = SnowflakeToBQPipeline(
                task_id=f"snowflake_pull_{normalized_view_name}_to_warehouse",
                xcom_task_id=f"source_{normalized_view_name}.get_source_config_{normalized_view_name}",
                view=view,
                runner="DataflowRunner",
                jar=SF_TO_BQ_JAR,
                tenant=TENANT,
                pipeline=PIPELINE,
                project="{{ var.value.get('gcp_project') }}",
                billing_project="{{ var.value.get('billing_project_id') }}",
                service_account="{{ var.value.get('service_account_email') }}",
                region="{{ var.value.get('region') }}",
                bigquery_region="{{ var.value.get('bigquery_region', 'US') }}",
                temp_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/temp/' }}",
                staging_location="{{ 'gs://' + var.value.get('gcp_bucket', 'sourcing_ingestion') + '/dataflow/staging/' }}",
                num_workers=5,
                max_num_workers=5,
                worker_machine_type="n2-standard-8",
                subnetwork="{{ var.value.get('subnetwork') }}",
                use_public_ips=False,
                bigquery_project="{{ var.value.get('gcp_project') }}",
                bigquery_dataset="{{ var.value.get('gcp_dataset') }}",
                bigquery_table=normalized_view_name
            )
            branch_p2_based_on_driver = BranchPythonOperator(
                task_id="select_gcs_to_warehouse_operator",
                python_callable=utils.select_gcs_to_warehouse_operator,
                op_kwargs={"view": view, "task_group_id": f"source_{normalized_view_name}"},
            )

            update_variable_if_success = PythonOperator(
                task_id=f"update_variable_if_success_{normalized_view_name}",
                python_callable=utils.set_variable_through_task,
                op_kwargs={"view": view},
                trigger_rule="none_failed_min_one_success",
            )

            update_extraction_sync_time = PythonOperator(
                task_id=f"update_extraction_sync_time_{normalized_view_name}",
                python_callable=utils.update_extraction_sync_dt,
                op_kwargs={"view": view},
                trigger_rule="none_failed_min_one_success",
                retries=6,
            )

            # create a end point
            end_task = DummyOperator(
                task_id=f"end_{normalized_view_name}", trigger_rule="none_failed_min_one_success"
            )

            (
                start_task
                >> get_source_config_view
                >> branch_p2_based_on_driver
                >> [db_to_warehouse, sftp_to_warehouse, gcs_to_warehouse_task, snowflake_to_warehouse]
                >> update_variable_if_success
                >> update_extraction_sync_time
                >> end_task
            )
        view_load_tasks.append(tg)
    for task_group in view_load_tasks:
        trigger >> task_group

    row_count_validation = PythonOperator(
        task_id="row_count_validation",
        python_callable=row_count_validation_obj.row_count_validation,
    )
    view_load_tasks >> row_count_validation
    row_count_validation >> send_notification_mail

    slack_notification_task = PythonOperator(
        task_id="slack_notification",
        trigger_rule="all_done",
        python_callable=slack__on_task_failure,
        provide_context=True,
    )

    row_count_validation >> slack_notification_task
    end_dag_task = DummyOperator(task_id="end_dag", trigger_rule="none_failed")

    remote_dag_trigger = PythonOperator(
        task_id="trigger_intermediate_queries_dag",
        trigger_rule="all_success",
        python_callable=trigger_remote_scheduling_dag,
    )

    branching1 = BranchPythonOperator(
        task_id="branching1",
        python_callable=decide_branch_for_extraction,
        dag=dag,
    )
    branching2 = BranchPythonOperator(
        task_id="branching2",
        python_callable=decide_trigger_intermediate_dag,
        dag=dag,
    )

    validate_intermediate_queries = ValidateIntermediateOperator(
        task_id="validate_start_of_intermediate_queries",
        extraction_flag="{{ var.value.get('extraction_flag') }}",
        trigger_rule="all_success",
    )

    trigger_sourcing_dag = TriggerDagRunOperator(
        task_id="trigger_sourcing_dag_on_same_machine",
        trigger_rule="all_success",
        trigger_dag_id="sourcing_intermediate_queries",
        dag=dag,
    )

    (
        sourcing_start
        >> load_env_vars_sourcing
        >> add_warehouse_connection
        >> branching1
        >> [fetch_the_sourcing_parameters, validate_intermediate_queries, end_dag_task]
    )
    fetch_the_sourcing_parameters >> trigger

    if not (view_load_tasks):
        no_load_tasks = DummyOperator(task_id="Dummy__no_load_tasks")
        (
            trigger
            >> no_load_tasks
            >> [slack_notification_task, send_notification_mail, branching2]
        )

    (branching2 >> [validate_intermediate_queries, end_dag_task])
    (
        validate_intermediate_queries
        >> trigger_sourcing_dag
        >> remote_dag_trigger
        >> end_dag_task
    )

    row_count_validation >> branching2
