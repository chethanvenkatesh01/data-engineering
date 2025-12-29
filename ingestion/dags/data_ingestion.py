import os
import json
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta

from constants import constant as constants
from constants.constant import (
    GBQ_DB_COPY_MULTI_TABLES_JAR,
    GBQ_DB_COPY_TABLES_JAR,
    NotificationType,
    DB_GBQ_COPY_TABLES_JAR,
)
from constants.load_variables import load_initial_variables

# from constants.dynamic_constants import entities as entities_const

"""testing this"""
# IMPORT AIRFLOW IMPORTS
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from operators.GenericDataWarehouseOperator import GenericDataWarehouseOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
    BeamRunPythonPipelineOperator,
)

# IMPORT TASKS/UTILS
from tasks import utils as ut
from tasks.day_split import DaySplit
from tasks import connection as conn
from tasks.containers.class_A import A
from tasks.containers.class_B import B
from tasks import slack_integration as sl
from tasks import lost_sales_imputation as lsi
from tasks.aggregate_table import AggregateTable
from tasks.clustering import store_clustering
from constants.constant import get_overall_config

from tasks.master_table import generate_query
from tasks.fiscal_table import generate_fiscal_query
from tasks.derived_table import trigger_python_script
from tasks.store_split import StoreSplitClass, find_hierarchy_count
from tasks.qc_module import (
    query_formation,
    on_success_qc_callback,
    clear_qc_completion_table_list,
    post_derived_table_qc_run,
)
from tasks.notification_summary import (
    send_notification,
    send_validation_notification,
)
from tasks.vm_start_stop import call_vm_stop_api
from tasks.warehouse_to_db_operator_selector import WareHouseToDbOperatorSelector
from tasks.db_to_warehouse_operator_selector import DbtoWareHouseOperatorSelector
from queries import query_builder as qb

overall_config = get_overall_config()

ingestion_config = overall_config["ingestion_config"]
entities_config = overall_config["entities_config"]
der_tab_config = overall_config["der_tab_config"]

default_args = {
    "owner": ingestion_config.get("owner", "IA"),
    "retries": ingestion_config.get("retries", 1),
    "retry_delay": timedelta(seconds=ingestion_config.get("retry_delay_sec", 20)),
    "email_on_failure": ingestion_config.get("email_on_failure"),
    "email_on_retry": ingestion_config.get("email_on_retry"),
    "project_id": ingestion_config.get("gcp_project"),
}

dataflow_default_options = {
    "project": ingestion_config.get("gcp_project"),
    "runner": ingestion_config.get("runner", "DataFlowRunner"),
    "region": ingestion_config.get("region"),
    "temp_location": ingestion_config.get("tempbucket"),
    "direct_num_workers": ingestion_config.get("direct_num_workers"),
    "max_num_workers": ingestion_config.get("max_num_workers"),
    "network": ingestion_config.get("network"),
    "subnetwork": ingestion_config.get("subnetwork"),
    "no_use_public_ips": ingestion_config.get("no_use_public_ips"),
    "machine_type": ingestion_config.get("machine_type"),
}

BASE_DIR = Path(__file__).resolve(strict=True).parent
DATAFLOW_COPY_FILE_BQ_PG = BASE_DIR / "tasks/copy_gbq_to_psg.py"
DATAFLOW_COPY_FILE_PG_BQ = BASE_DIR / "tasks/copy_psg_to_gbq.py"


with DAG(
    dag_id="data_ingestion",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2021, 1, 1),
) as dag:

    warehouse = "{{ var.value.get('warehouse') }}"

    load_env_vars_ingestion = PythonOperator(
        task_id="load_env_variables_ingestion",
        python_callable=load_initial_variables,
    )

    validation_starts = DummyOperator(
        task_id="Ingestion",
        trigger_rule="all_done",
        on_success_callback=sl.slack__first_task_success,
    )

    stop_sourcing_vm_instance = PythonOperator(
        task_id="stop_sourcing_vm_instance",
        python_callable=call_vm_stop_api,
        op_kwargs={
            "instance_name": "{{var.value.get('tenant_alias')}}-sourcing-{{var.value.get('pipeline')}}"
        },
    )

    validation_ends = PythonOperator(
        task_id="validation_ends",
        trigger_rule="all_success",
        python_callable=send_validation_notification,
    )

    check_fmt_cadence = PythonOperator(
        task_id="check_fmt_cadence",
        python_callable=ut.check_if_to_run,
        op_kwargs={
            "task_name": "fmt",
            "condition": "{{var.value.get('fmt_cadence', 'False')}}",
        },
    )

    check_master_table_cadence = PythonOperator(
        task_id="check_master_table_cadence",
        python_callable=ut.check_if_to_run,
        op_kwargs={
            "task_name": "master_table",
            "condition": "{{var.value.get('main_master_cadence', 'False')}}",
        },
    )

    if ingestion_config.get("generate_mp_bucket_code", False):
        generate_mp_bucket_code = PythonOperator(
            task_id="generate_mp_bucket_code_for_product_validated",
            python_callable=ut.generate_mp_bucket_code,
        )
    else:
        generate_mp_bucket_code = DummyOperator(
            task_id="generate_mp_bucket_code_for_product_validated",
            trigger_rule="all_success",
        )

    persistence_starts = DummyOperator(
        task_id="persistence_starts", trigger_rule="all_success"
    )
    
    ensure_exclusive_db_flow_start = PostgresOperator(
        task_id="ensure_exclusive_db_flow_start",
        sql="call public.ensure_exclusive_db_flow(false, 'Data Ingestion'); call public.ensure_exclusive_db_flow(true, 'Data Ingestion');",
        retries=5,
        retry_delay=timedelta(minutes=3),
    )

    persistence_ends = DummyOperator(
        task_id="persistence_ends", trigger_rule="all_success"
    )

    copy_hierarchy_table_gbq = DbtoWareHouseOperatorSelector(
        task_id=f"copy_product_hierarchy_filter_flattened_to_warehouse",
        db_table="product_hierarchies_filter_flattened",
        db_type="postgresql",
        replace_table="true",
        data_flow_job_name=f"{ingestion_config.get('tenant')}-{ingestion_config.get('pipeline')}-copy-product-hierarchies-filter-flattened-psg-to-warehouse",
        warehouse_table="product_hierarchies_filter_flattened",
        num_workers=1,
        max_num_workers=5,
        ingestion_config=ingestion_config,
    )
    
    copy_store_hierarchy_table_gbq = DbtoWareHouseOperatorSelector(
        task_id=f"copy_store_hierarchy_filter_flattened_to_warehouse",
        db_table="store_hierarchies_filter_flattened",
        db_type="postgresql",
        replace_table="true",
        data_flow_job_name=f"{ingestion_config.get('tenant')}-{ingestion_config.get('pipeline')}-copy-store-hierarchies-filter-flattened-psg-to-warehouse",
        warehouse_table="store_hierarchies_filter_flattened",
        num_workers=1,
        max_num_workers=5,
        ingestion_config=ingestion_config,
    )

    preprocessing_starts = DummyOperator(
        task_id="preprocessing_starts",
        trigger_rule="all_success",
        on_success_callback=sl.slack__on_task_success,
    )

    preprocessing_ends = DummyOperator(
        task_id="preprocessing_ends",
        trigger_rule="all_success",
        on_success_callback=sl.slack__last_task_success,
    )

    t_gcp = PythonOperator(
        task_id="add_warehouse_connection_python",
        python_callable=conn.add_warehouse_connection,
        provide_context=True,
    )

    t_psg = PythonOperator(
        task_id="add_psg_connection_python",
        python_callable=conn.add_psg_connection,
        provide_context=True,
    )

    delete_public_tables = PostgresOperator(
        task_id="delete_tables_in_postgres_public",
        sql="call public.schema_cleanup();",
    )

    t0 = PythonOperator(
        task_id="generate_ingestion_table_list",
        python_callable=ut.generic_mapping_data,
    )

    each_master_updated = DummyOperator(
        task_id="each_master_updated", trigger_rule="all_success"
    )

    data_loss_check = PythonOperator(
        task_id="checking_data_loss",
        python_callable=ut.set_anomaly_tables_list,
        trigger_rule="all_success",
    )

    cv_started = DummyOperator(
        task_id="get_tables_to_cross_validate", trigger_rule="all_success"
    )

    cv_ended = DummyOperator(
        task_id="cross_validation_ended", trigger_rule="all_success"
    )

    today = date.today()
    today = str(today).replace("-", "_")

    attribute_table_list = entities_config["attribute_table_list"]
    attribute_hierarchy_list = entities_config["attribute_hierarchy_list"]

    for et, rt, mt, dt, pt, dlt in zip(
        entities_config["entity_type"],
        entities_config["raw_table"],
        entities_config["mapping_table"],
        entities_config["destination_table"],
        entities_config["pull_type"],
        entities_config["data_loss_threshold"],
    ):

        validated_table = f"{et}_validated_table"
        level = Variable.get(f"level_{et}", default_var="")
        args = (et, rt, validated_table, attribute_hierarchy_list, level, dt, pt)

        if et in attribute_hierarchy_list:
            validation = B(*args)
        else:
            validation = A(*args)

        t1 = PythonOperator(
            task_id=f"Read_Mapping_Data_{et}",
            python_callable=ut.read_mapping_data,
            op_kwargs={
                "raw_table": rt,
                "mapping_table": mt,
                "entity_type": et,
                "pull_type": pt,
                "data_loss_threshold": dlt,
            },
        )

        t2 = PythonOperator(
            task_id=f"generate_transformed_validated_query_{et}",
            python_callable=validation.transform_validate,
        )

        t3 = GenericDataWarehouseOperator(
            task_id=f"create_transformation_and_validation_{et}_table",
            sql=f"""{{{{ var.value.get("{et}_transformation_validation_query") }}}}""",
            conn_id="warehouse_connection",
            warehouse="""{{ var.value.get("warehouse") }}""",
            # extras = """{{var.value.get("warehouse_kwargs")}}"""
        )

        qc = PythonOperator(
            task_id=f"running_qc_on_{et}_table",
            python_callable=query_formation,
            op_kwargs={
                "stage": "table_specific",
                "table": et,
                "mod_name": "validation",
            },
        )

        initial_task = (
            stop_sourcing_vm_instance
            >> validation_starts
            >> load_env_vars_ingestion
            >> t_gcp
            >> t_psg
            >> delete_public_tables
            >> t0
            >> t1
            >> t2
            >> t3
            >> qc
            >> cv_started
        )

    data_loss_check_done = DummyOperator(
        task_id="data_loss_check_done", trigger_rule="all_success"
    )

    for et, rt, mt, dt, pt, dlt in zip(
        entities_config["entity_type"],
        entities_config["raw_table"],
        entities_config["mapping_table"],
        entities_config["destination_table"],
        entities_config["pull_type"],
        entities_config["data_loss_threshold"],
    ):
        cv_t4 = PythonOperator(
            task_id=f"running_cross_validation_qc_on_{et}_table",
            python_callable=query_formation,
            op_kwargs={
                "stage": "cross_validation",
                "table": et,
                "mod_name": "validation",
            },
        )
        task_cv = initial_task >> cv_t4 >> cv_ended >> data_loss_check

    for et in entities_config["entity_type"]:
        anomaly_check_task = PythonOperator(
            task_id=f"check_data_loss_{et}",
            python_callable=ut.data_anomaly_check,
            op_kwargs={"entity_type": et},
        )
        check_task = (
            task_cv >> anomaly_check_task >> data_loss_check_done  # >> validation_ends
        )

    export_anomaly_data_to_gcs = True
    if export_anomaly_data_to_gcs:

        for et in entities_config["entity_type"]:

            anomaly_export_query = PythonOperator(
            task_id=f"generate_{et}_anomaly_to_gcs_query",
            python_callable=qb.build_export_anomaly_data_to_gcs_query,
            op_kwargs={"entity": et},
          )

            anomaly_data_to_gcs = GenericDataWarehouseOperator(
                task_id=f"copy_{et}_anomaly_data_to_gcs",
                sql=f"{{{{var.value.get('{et}_anomaly_to_gcs_query', '')}}}}",
                conn_id="warehouse_connection",
                warehouse="""{{ var.value.get("warehouse") }}""",
                extras={"split_lines": False},
            )

            data_loss_check_done >> anomaly_export_query >> anomaly_data_to_gcs >> validation_ends
    else:
        data_loss_check_done >> validation_ends

    for et, rt, mt, dt, pt, pgp in zip(
        entities_config["entity_type"],
        entities_config["raw_table"],
        entities_config["mapping_table"],
        entities_config["destination_table"],
        entities_config["pull_type"],
        entities_config["pg_persistence"],
    ):

        validated_table = f"{et}_validated_table"
        level = Variable.get(f"level_{et}", default_var="")
        args = (et, rt, validated_table, attribute_hierarchy_list, level, dt, pt)

        if et in attribute_hierarchy_list:
            validation = B(*args)
        else:
            validation = A(*args)

        mapping_data_et = eval(
            Variable.get(f"mapping_data_{et}", default_var="{}").replace("nan", "None")
        )
        mapping_data_et_df = pd.DataFrame(
            {
                key: mapping_data_et[key]
                for key in ["generic_column_name", "generic_column_datatype"]
                if key in mapping_data_et
            }
        )
        if len(mapping_data_et_df) > 0:
            schema_from_generic_mapping = str(
                dict(
                    zip(
                        mapping_data_et_df["generic_column_name"].tolist(),
                        mapping_data_et_df["generic_column_datatype"].tolist(),
                    )
                )
            )
        else:
            schema_from_generic_mapping = "{}"

        schema_from_generic_mapping = schema_from_generic_mapping.replace("'", '"')

        if pgp:
            options = dataflow_default_options.copy()
            options["job_name"] = (
                f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-validated-table-warehouse-to-psg-{et.replace('_','-')}"
            )
            if (
                isinstance(validation, B)
                and ingestion_config.get("ingestion_type") == "periodic"
            ):
                source_bq_table = f"{et}_delta_table"
                destination_db_table = f"{et}_delta_table"
                create_delta_table_bq = PythonOperator(
                    task_id=f"create_delta_table_{et}",
                    python_callable=ut.create_delta_table_bq,
                    op_kwargs={"entity": f"{et}"},
                )

                create_delta_attributes_table_bq = PythonOperator(
                    task_id=f"create_delta_attributes_table_{et}",
                    python_callable=ut.create_delta_attributes_table_bq,
                    op_kwargs={"entity": f"{et}"},
                )
                # store the record type as int2 field while pushing delta
                schema_from_generic_mapping = eval(schema_from_generic_mapping)
                schema_from_generic_mapping["__record_type"] = "int2"
                schema_from_generic_mapping = str(schema_from_generic_mapping)
                schema_from_generic_mapping = schema_from_generic_mapping.replace(
                    "'", '"'
                )

                copy_delta_table = WareHouseToDbOperatorSelector(
                    task_id=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-delta-table-from-warehouse-to-psg-{et}",
                    db_type="postgresql",
                    db_schema_name="public",
                    data_flow_job_name=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-delta-table-from-warehouse-to-psg-{et.replace('_','-')}",
                    single_table_config={
                        "warehouse_table": source_bq_table,
                        "db_table": destination_db_table,
                        "replace_table": "true",
                        "db_table_schema": schema_from_generic_mapping,
                    },
                    num_workers=1,
                    max_num_workers=7,
                    ingestion_config=ingestion_config,
                )

                copy_attributes_delta_table = WareHouseToDbOperatorSelector(
                    task_id=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-attributes-delta-warehouse-to-psg-{et}",
                    data_flow_job_name=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-attributes-delta-warehouse-to-psg-{et.replace('_','-')}",
                    single_table_config={
                        "warehouse_table": f"{et}_attributes_delta_table",
                        "replace_table": "false",  # "true",
                        "db_table": f"{et}_attributes_delta_table",
                        "db_table_schema": schema_from_generic_mapping,
                    },
                    db_type="postgresql",
                    db_schema_name="public",
                    num_workers=1,
                    max_num_workers=7,
                    ingestion_config=ingestion_config,
                )

                copy_validated_table = WareHouseToDbOperatorSelector(
                    task_id=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-validated-table-warehouse-to-psg-{et}",
                    data_flow_job_name=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-validated-table-warehouse-to-psg-{et.replace('_','-')}",
                    single_table_config={
                        "warehouse_table": f"{et}_validated_table",
                        "replace_table": "true",
                        "db_table": f"{et}_validated_table",
                        "db_table_schema": schema_from_generic_mapping,
                    },
                    db_type="postgresql",
                    db_schema_name="public",
                    num_workers=1,
                    max_num_workers=7,
                    ingestion_config=ingestion_config,
                )

                copy_table = [
                    copy_delta_table,
                    copy_attributes_delta_table,
                    copy_validated_table,
                ]

            elif (
                isinstance(validation, B)
                and ingestion_config.get("ingestion_type") != "periodic"
            ):
                source_bq_table = f"{et}_validated_table"
                destination_db_table = f"{et}_validated_table"
                create_delta_table_bq = DummyOperator(
                    task_id=f"create_delta_table_{et}", trigger_rule="all_success"
                )

                create_delta_attributes_table_bq = PythonOperator(
                    task_id=f"create_delta_attributes_table_{et}",
                    python_callable=ut.create_delta_attributes_table_bq,
                    op_kwargs={"entity": f"{et}"},
                )

                copy_table = WareHouseToDbOperatorSelector(
                    task_id=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-validated-table-warehouse-to-psg-{et}",
                    data_flow_job_name=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-validated-table-warehouse-to-psg-{et.replace('_','-')}",
                    single_table_config={
                        "warehouse_table": source_bq_table,
                        "replace_table": "true",
                        "db_table": destination_db_table,
                        "db_table_schema": schema_from_generic_mapping,
                    },
                    db_type="postgresql",
                    db_schema_name="public",
                    num_workers=1,
                    max_num_workers=7,
                    ingestion_config=ingestion_config,
                )

            else:
                source_bq_table = f"{et}_validated_table"
                destination_db_table = f"{et}_validated_table"

                create_delta_table_bq = DummyOperator(
                    task_id=f"create_delta_table_{et}", trigger_rule="all_success"
                )

                create_delta_attributes_table_bq = DummyOperator(
                    task_id=f"create_delta_attributes_table_{et}",
                    trigger_rule="all_success",
                )

                copy_table = WareHouseToDbOperatorSelector(
                    task_id=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-validated-table-warehouse-to-psg-{et}",
                    data_flow_job_name=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-validated-table-warehouse-to-psg-{et.replace('_','-')}",
                    single_table_config={
                        "warehouse_table": source_bq_table,
                        "replace_table": "true",
                        "db_table": destination_db_table,
                        "db_table_schema": schema_from_generic_mapping,
                    },
                    db_type="postgresql",
                    num_workers=1,
                    max_num_workers=7,
                    db_schema_name="public",
                    ingestion_config=ingestion_config,
                )

        else:
            create_delta_table_bq = DummyOperator(
                task_id=f"pass_delta_table_creation_for_pg_{et}",
                trigger_rule="all_success",
            )
            create_delta_attributes_table_bq = DummyOperator(
                task_id=f"pass_delta_attribute_table_creation_for_pg_{et}",
                trigger_rule="all_success",
            )
            copy_table = DummyOperator(
                task_id=f"pass_postgres_persistence_{et}", trigger_rule="all_success"
            )

        fetch_optimization_clause = PythonOperator(
            task_id=f"fetch_optimization_clause_{et}",
            python_callable=ut.get_unique_partition_values,
            op_kwargs={"entity_type": et},
        )

        t6 = PythonOperator(
            task_id=f"generate_queries_to_create_tables_{et}",
            python_callable=validation.create_tables,
        )

        if ingestion_config.get("ingestion_type") != "periodic":
            if et not in attribute_hierarchy_list:
                t7 = GenericDataWarehouseOperator(
                    task_id=f"create_master_table_{et}",
                    sql=f"{{{{var.value.get('{et}_master_table_query_np_f', '')}}}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    # extras = """{{var.value.get("warehouse_kwargs")}}"""
                )
                if pgp:
                    t7_p = PostgresOperator(
                        task_id=f"create_master_table_postgres_{et}",
                        # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                        sql=f"{{{{var.value.get('{et}_master_table_query_np_f_pg', '')}}}}",
                    )

                    t_7_1 = [t7, t7_p]
                else:
                    t_7_1 = [t7]

            else:
                t7 = GenericDataWarehouseOperator(
                    task_id=f"create_flatten_master_table_{et}",
                    sql=f"{{{{var.value.get('{et}_master_table_query_np_f', '')}}}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    # extras = """{{var.value.get("warehouse_kwargs")}}"""
                )

                t8 = PostgresOperator(
                    task_id=f"create_master_table_{et}",
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    sql=f"{{{{var.value.get('{et}_master_table_query_np', '')}}}}",
                )

                tta = PostgresOperator(
                    task_id=f"create_time_attributes_table_{et}",
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    sql=f"{{{{var.value.get('{et}_time_attributes_table_query_np', '')}}}}",
                )

                t9 = PostgresOperator(
                    task_id=f"create_attribute_table_{et}",
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    sql=f"{{{{var.value.get('{et}_attribute_table_query_np', '')}}}}",
                )

                set_maintenance_flag_task = PostgresOperator(
                    task_id=f"set_maintenance_flag_{et}",
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    sql="call public.set_db_maintenance(true);",
                )

                attributes_filter_table = PostgresOperator(
                    task_id=f"create_attributes_filter_table_{et}",
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    sql=f"{{{{var.value.get('{et}_attributes_filter_sp', '')}}}}",
                )

                reset_maintenance_flag_task = PostgresOperator(
                    task_id=f"reset_maintenance_flag_{et}",
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    sql="call public.set_db_maintenance(false);",
                )

                hierarchies_filter_table = PostgresOperator(
                    task_id=f"create_hierarchies_filter_table_{et}",
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    sql=f"{{{{var.value.get('{et}_hierarchies_filter_sp', '')}}}}",
                )

                t_7_1 = (
                    fetch_optimization_clause
                    >> t6
                    >> t7
                    >> t8
                    >> tta
                    >> t9
                    >> set_maintenance_flag_task
                    >> attributes_filter_table
                    >> reset_maintenance_flag_task
                    >> hierarchies_filter_table
                )

        else:
            if et not in attribute_hierarchy_list:
                t7 = GenericDataWarehouseOperator(
                    task_id=f"append_master_table_{et}",
                    sql=f"{{{{ var.value.get('{et}_master_table_query_p', '') }}}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    # extras = """{{var.value.get("warehouse_kwargs")}}"""
                )
                if pgp:
                    t7_p = PostgresOperator(
                        task_id=f"append_master_table_postgres_{et}",
                        # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                        sql=f"{{{{var.value.get('{et}_master_table_query_p_f_pg', '')}}}}",
                    )

                    t_7_1 = [t7, t7_p]
                else:
                    t_7_1 = [t7]

            else:
                t7 = GenericDataWarehouseOperator(
                    task_id=f"update_flatten_master_table_{et}",
                    sql=f"{{{{ var.value.get('{et}_master_table_query_p_f') }}}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    # extras = """{{var.value.get("warehouse_kwargs")}}"""
                )

                t_delta_sync = PostgresOperator(
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    task_id=f"sync_delta_{et}_to_db",
                    sql=f"call public.sync_{et}_master_delta()",
                )

                tta = PostgresOperator(
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    task_id=f"create_time_attributes_table_{et}",
                    sql=f"{{{{var.value.get('{et}_time_attributes_table_query_p', '')}}}}",
                )

                t9 = PostgresOperator(
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    task_id=f"create_attribute_table_{et}",
                    sql=f"call global.build_{et}_attributes_delta()",
                )

                set_maintenance_flag_task = PostgresOperator(
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    task_id=f"set_maintenance_flag_{et}",
                    sql="call public.set_db_maintenance(true);",
                )

                attributes_filter_table = PostgresOperator(
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    task_id=f"create_attributes_filter_table_{et}",
                    sql=f"{{{{var.value.get('{et}_attributes_filter_sp', '')}}}}",
                )

                reset_maintenance_flag_task = PostgresOperator(
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    task_id=f"reset_maintenance_flag_{et}",
                    sql="call public.set_db_maintenance(false);",
                )

                hierarchies_filter_table = PostgresOperator(
                    # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                    task_id=f"create_hierarchies_filter_table_{et}",
                    sql=f"{{{{var.value.get('{et}_hierarchies_filter_sp', '')}}}}",
                )

                t_7_1 = (
                    fetch_optimization_clause
                    >> t6
                    >> t7
                    >> t_delta_sync
                    >> tta
                    >> t9
                    >> set_maintenance_flag_task
                    >> attributes_filter_table
                    >> reset_maintenance_flag_task
                    >> hierarchies_filter_table
                )

        if Variable.get(f"pull_type_{et}", default_var="default") == "incremental":
            fetch_latest_syncdate = PythonOperator(
                task_id=f"fetch_latest_sync_date_{et}",
                python_callable=ut.fetch_latest_sync_date,
                op_kwargs={"et": et, "rt": rt},
            )

            latest_syncdate_update_query = PythonOperator(
                task_id=f"{et}_get_update_latest_sync_date_update",
                python_callable=ut.update_to_latest_syncdate_udpate,
                op_kwargs={"et": et, "rt": rt},
            )
            latest_syncdate_update = GenericDataWarehouseOperator(
                task_id=f"latest_syncdate_update_{et}",
                sql=f"{{{{var.value.get('update_mapping_table_with_latest_date_query_{et}','dummy_query')}}}}",
                conn_id="warehouse_connection",
                warehouse="""{{ var.value.get("warehouse") }}""",
            )

        else:
            fetch_latest_syncdate = DummyOperator(
                task_id=f"dummy_fetch_latest_sync_date_{et}", trigger_rule="all_success"
            )
            latest_syncdate_update_query = DummyOperator(
                task_id=f"dummy_{et}_get_udpate_latest_sync_date_udpate",
                trigger_rule="all_success",
            )
            latest_syncdate_update = DummyOperator(
                task_id=f"dummy_latest_syncdate_update_{et}", trigger_rule="all_success"
            )

        task1 = (
            create_delta_table_bq
            >> create_delta_attributes_table_bq
            >> copy_table
            >> fetch_optimization_clause
            >> t6
            >> t_7_1
            >> fetch_latest_syncdate
            >> latest_syncdate_update_query
            >> latest_syncdate_update
        )

        persistence_starts >> create_delta_table_bq
        latest_syncdate_update >> each_master_updated

    (
        validation_ends
        >> generate_mp_bucket_code
        >> ensure_exclusive_db_flow_start
        >> persistence_starts
        >> task1
        >> each_master_updated
        >> [copy_hierarchy_table_gbq, copy_store_hierarchy_table_gbq]
        >> persistence_ends
        >> check_fmt_cadence
        >> check_master_table_cadence
        >> preprocessing_starts
    )

    # ============================================================================= Derived tables =======================================================
    set_derived_tables_variables = PythonOperator(
        task_id="set_derived_tables_variables",
        python_callable=ut.fetch_derived_tables_data,
        on_success_callback=sl.slack__on_task_success,
    )
    preprocessing_starts >> set_derived_tables_variables

    # der_tab = ut.read_json_file('derived_tables.json')
    der_tab = der_tab_config
    der_tabs_names_list = der_tab["der_tabs_exec_order_dict"]
    der_execution_order_dict = der_tab["der_tabs_exec_order_dict"]
    der_tabs_info_dict = der_tab["der_tabs_info_dict"]
    der_tabs_copy_to_psg_job_position = der_tab["der_tabs_copy_to_psg_job_position"]

    # Run the derived tables

    der_execution_order_dict = der_tab["der_tabs_exec_order_dict"]

    der_tables_to_copy_to_psg = []
    tasks_in_the_order = {}
    for order in der_execution_order_dict:

        tasks_in_current_order = []
        for table_name in der_execution_order_dict[order]:

            # -- task groups
            with TaskGroup(group_id=f"dt__{table_name}") as tg:

                # -- run in : gbq
                if der_tabs_info_dict[table_name]["run_in"] == "gbq":
                    # if (Variable.get(f"der_tab_{table_name}_run_in", default_var="") == "gbq"):

                    gb_table_name = table_name + "_validated_table"
                    # if (
                    #     ingestion_config.get("ingestion_type") == "periodic"
                    #     and der_tabs_info_dict[table_name]["type"] != "procedure"
                    #     # Variable.get(f"der_tab_{table_name}_type", default_var="")!= "procedure"
                    # ):
                    #     if (
                    #         der_tabs_info_dict[table_name]["replace_flag_gbq"]
                    #         == "False"
                    #     ):
                    #         # if (Variable.get(f"der_tab_{table_name}_replace_flag_gbq",default_var="True",)== "False"):
                    #         update_main_table_query_gbq = f"""
                    #             insert into {ingestion_config.get('gcp_project')}.{ingestion_config.get('dataset')}.{table_name}
                    #             (select * from {ingestion_config.get('gcp_project')}.{ingestion_config.get('dataset')}.{gb_table_name})
                    #         """
                    #     elif (
                    #         der_tabs_info_dict[table_name]["replace_flag_gbq"] == "True"
                    #     ):
                    #         # elif (Variable.get(f"der_tab_{table_name}_replace_flag_gbq",default_var="True",)== "True"):
                    #         update_main_table_query_gbq = f"""
                    #             select * from {ingestion_config.get('gcp_project')}.{ingestion_config.get('dataset')}.{gb_table_name}
                    #         """
                    # elif (
                    #     ingestion_config.get("ingestion_type") == "non-periodic"
                    #     and der_tabs_info_dict[table_name]["type"] != "procedure"
                    #     # Variable.get(f"der_tab_{table_name}_type", default_var="")!= "procedure"
                    # ):
                    #     update_main_table_query_gbq = f"""
                    #         select * from {ingestion_config.get('gcp_project')}.{ingestion_config.get('dataset')}.{gb_table_name}
                    #     """

                    get_update_query = PythonOperator(
                        task_id=f"get_update_query_{table_name}",
                        python_callable=ut.get_update_query_derived_table,
                        op_kwargs={
                            "table_name": table_name,
                            "gb_table_name": gb_table_name,
                        },
                    )
                    # -- type : query

                    if der_tabs_info_dict[table_name]["type"] == "query":
                        # if (Variable.get(f"der_tab_{table_name}_type", default_var="")== "query"):
                        create_derived_table = GenericDataWarehouseOperator(
                            task_id=f"create_{table_name}_derived_table_validated",
                            sql=f"{{{{var.value.get('der_tab_{table_name}_query', '')}}}}",
                            destination_dataset_table=gb_table_name,
                            write_disposition="WRITE_TRUNCATE",
                            conn_id="warehouse_connection",
                            warehouse="""{{ var.value.get("warehouse") }}""",
                            # extras = """{{var.value.get("warehouse_kwargs")}}"""
                        )
                        if der_tabs_info_dict[table_name]["replace_flag_psg"] in (
                            "True",
                            "False",
                        ):
                            # if (Variable.get(f"der_tab_{table_name}_replace_flag_psg",default_var="None",) in ("True", "False")):
                            der_tables_to_copy_to_psg.append(
                                f"{gb_table_name}:{table_name}"
                            )

                        if (
                            ingestion_config.get("ingestion_type") == "non-periodic"
                            or der_tabs_info_dict[table_name]["replace_flag_gbq"]
                            == "True"
                            # Variable.get(f"der_tab_{table_name}_replace_flag_gbq",default_var="True",)== "True"
                        ):

                            update_main_table = GenericDataWarehouseOperator(
                                task_id=f"update_{table_name}_derived_table",
                                sql=f"{{{{var.value.get('update_main_table_query_gbq_{table_name}', '')}}}}",
                                destination_dataset_table=table_name,
                                write_disposition="WRITE_TRUNCATE",
                                conn_id="warehouse_connection",
                                warehouse="""{{ var.value.get("warehouse") }}""",
                                # extras = """{{var.value.get("warehouse_kwargs")}}"""
                            )
                        elif (
                            der_tabs_info_dict[table_name]["replace_flag_gbq"]
                            == "False"
                        ):
                            # elif (Variable.get(f"der_tab_{table_name}_replace_flag_gbq",default_var="True",)== "False"):
                            update_main_table = GenericDataWarehouseOperator(
                                task_id=f"insert_{table_name}_gbq",
                                sql=f"{{{{var.value.get('update_main_table_query_gbq_{table_name}', '')}}}}",
                                conn_id="warehouse_connection",
                                warehouse="""{{ var.value.get("warehouse") }}""",
                                destination_dataset_table=table_name,
                                write_disposition="WRITE_APPEND",
                                # extras = """{{var.value.get("warehouse_kwargs")}}"""
                            )

                        else:
                            update_main_table = DummyOperator(
                                task_id=f"dummy_insert_{table_name}_gbq"
                            )

                        run_derived_qc = PythonOperator(
                            task_id=f"run_qc_on_{table_name}",
                            python_callable=on_success_qc_callback,
                            op_kwargs={
                                "tab_name": gb_table_name,
                                "entity_name": table_name,
                                "tab_location":"""{{ var.value.get("warehouse") }}""",
                                "mod_name": "derived_table",
                            },
                        )

                    # -- type : procedure
                    elif der_tabs_info_dict[table_name]["type"] == "procedure":
                        # elif (Variable.get(f"der_tab_{table_name}_type", default_var="")== "procedure"):

                        # call the procedure directly without setting destination table
                        create_derived_table = GenericDataWarehouseOperator(
                            task_id=f"create_{table_name}_derived_table",
                            sql=f"{{{{var.value.get('der_tab_{table_name}_query', '')}}}}",
                            conn_id="warehouse_connection",
                            warehouse="""{{ var.value.get("warehouse") }}""",
                            # extras = """{{var.value.get("warehouse_kwargs")}}"""
                        )
                        update_main_table = DummyOperator(
                            task_id=f"dummy_insert_{table_name}_gbq"
                        )
                        run_derived_qc = DummyOperator(
                            task_id=f"dummy_qc_{table_name}_gbq"
                        )

                    (
                        create_derived_table
                        >> get_update_query
                        >> run_derived_qc
                        >> update_main_table
                    )

                # -- run in : psg
                elif der_tabs_info_dict[table_name]["run_in"] == "psg":
                    # elif (Variable.get(f"der_tab_{table_name}_run_in", default_var="")== "psg"):
                    
                    if der_tabs_info_dict[table_name]["type"] == "query_non_transactional":
                        create_derived_table = PostgresOperator(
                            task_id=f"run_psg_{table_name}_query",
                            # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                            sql=f"{{{{var.value.get('der_tab_{table_name}_query', '')}}}}",
                            autocommit=True
                        )
                    else:
                        create_derived_table = PostgresOperator(
                            task_id=f"run_psg_{table_name}_query",
                            # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                            sql=f"{{{{var.value.get('der_tab_{table_name}_query', '')}}}}",
                        )

                    if der_tabs_info_dict[table_name]["replace_flag_gbq"] in (
                        "True",
                        "False",
                    ):
                        # if (Variable.get(f"der_tab_{table_name}_replace_flag_gbq",default_var="None",)in ("True", "False")):
                        options = dataflow_default_options.copy()
                        options["job_name"] = (
                            f"der-tab-{table_name.replace('_','-').lower()}-copy-psg-to-gbq-{{var.value.get('tenant')}}-{{var.value.get('pipeline')}}"
                        )

                        copy_table = DbtoWareHouseOperatorSelector(
                            task_id=f"copy_{table_name.lower()}_to_warehouse",
                            data_flow_job_name=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-copy-{table_name.lower().replace('_', '-')}-psg-warehouse",
                            db_table=f"{table_name}",
                            warehouse_table=f"{table_name}",
                            db_type="postgresql",
                            replace_table=der_tabs_info_dict[table_name][
                                "replace_flag_gbq"
                            ],
                            num_workers=1,
                            max_num_workers=5,
                            ingestion_config=ingestion_config,
                        )
                        create_derived_table >> copy_table
                    else:
                        create_derived_table

                # run in : python(api call)
                elif der_tabs_info_dict[table_name]["run_in"] == "python":
                    # elif (Variable.get(f"der_tab_{table_name}_run_in", default_var="")== "python"):

                    call_the_api = PythonOperator(
                        task_id=f"trigger_python_script_{table_name}",
                        python_callable=trigger_python_script,
                        op_kwargs={
                            "task": table_name,
                            "arguments": f"{{{{var.value.get('der_tab_{table_name}_query', '')}}}}",
                        },
                    )
                    call_the_api

            tasks_in_current_order.append(tg)

        tasks_in_the_order[order] = tasks_in_current_order

    starting_derived_tables = DummyOperator(
        task_id="starting_derived_tables",
    )

    set_derived_tables_variables >> starting_derived_tables

    for order in tasks_in_the_order:
        derived_table_connector = DummyOperator(task_id=f"dt__{str(order)}")

        if (
            str(order) == str(der_tabs_copy_to_psg_job_position)
            and len(der_tables_to_copy_to_psg) > 0
        ):
            # if str(order) == Variable.get("der_tabs_copy_to_psg_job_position", default_var="None"):

            copy_der_tables_gbq_to_psg = WareHouseToDbOperatorSelector(
                task_id=f"der-tables-copy-warehouse-to-psg-{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}",
                data_flow_job_name=f"der-tables-copy-multi-warehouse-to-psg-{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}",
                db_type="postgresql",
                db_schema_name="public",
                num_workers=5,
                max_num_workers=20,
                ingestion_config=ingestion_config,
                multiple_table_copy_config={
                    "warehouse_table_to_pg_table_name_map": ",".join(
                        der_tables_to_copy_to_psg
                    )
                },
            )

            copy_der_tables_gbq_to_psg.doc_json = json.dumps(der_tables_to_copy_to_psg)

            (
                starting_derived_tables
                >> derived_table_connector
                >> copy_der_tables_gbq_to_psg
                >> tasks_in_the_order[order]
            )
        else:
            (
                starting_derived_tables
                >> derived_table_connector
                >> tasks_in_the_order[order]
            )

        starting_derived_tables = tasks_in_the_order[order]

    set_derived_tables_variables = starting_derived_tables

    derived_tables_completed = PostgresOperator(
        task_id="derived_tables_completed",
        # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
        sql="call public.update_pg_sync_status('pg_sync');",
        on_success_callback=sl.slack__on_task_success,
    )

    ensure_exclusive_db_flow_end = PostgresOperator(
        task_id="ensure_exclusive_db_flow_end",
        sql="call public.ensure_exclusive_db_flow(false, 'Data Ingestion');",
        retries=5,
        retry_delay=timedelta(minutes=3),
        trigger_rule="all_done"
    )

    task_build_list_partitions = PostgresOperator(
        task_id="build_list_partitions",
        # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
        sql="call global.build_list_partitions('mtp_audit_log')",
    )

    send_mail_notification = PythonOperator(
        task_id="send_mail_notification",
        python_callable=send_notification,
        op_kwargs={"notification_type": NotificationType.SUCCESS.value},
    )

    invalidate_cache = PythonOperator(
        task_id="invalidate_cache",
        python_callable=ut.invalidate_cache,
        op_kwargs={"status": "completed"},
    )

    task_clear_qc_completion_table_list = PythonOperator(
        task_id="task_clear_qc_completion_table_list",
        python_callable=clear_qc_completion_table_list,
    )

    task_post_derived_table_qc_run = PythonOperator(
        task_id="task_post_derived_table_qc_run",
        python_callable=post_derived_table_qc_run,
    )

    log_dag_summary = PythonOperator(
        task_id="log_dag_sumary",
        python_callable=ut.log_dag_summary,
        provide_context=True,
    )

    if ("fiscal_date_mapping" not in entities_config["destination_table"]) and (
        ingestion_config.get("ingestion_type") == "non-periodic"
    ):

        fiscal_query_task = PythonOperator(
            task_id="fiscal_table_query", python_callable=generate_fiscal_query
        )

        fiscal_table_task = GenericDataWarehouseOperator(
                    task_id="create_fiscal_table",
                    sql="{{ var.value.get(fiscal_table_query, '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="fiscal_date_mapping",
                    write_disposition="WRITE_TRUNCATE"
                )

        # Set ensure_exclusive_db_flow_end to run after derived_tables_completed with trigger_rule="all_done"
        
        fiscal_task = (
            set_derived_tables_variables
            >> derived_tables_completed
            >> task_build_list_partitions
            >> send_mail_notification
            >> task_clear_qc_completion_table_list
            >> task_post_derived_table_qc_run
            >> fiscal_query_task
            >> fiscal_table_task
            >> log_dag_summary
        )

    else:
        fiscal_dummy = DummyOperator(
            task_id="fiscal_table_pass", trigger_rule="all_success"
        )

        fiscal_task = (
            set_derived_tables_variables
            >> derived_tables_completed
            >> task_build_list_partitions
            >> send_mail_notification
            >> task_clear_qc_completion_table_list
            >> task_post_derived_table_qc_run
            >> fiscal_dummy
            >> log_dag_summary
        )

    # if eval(Variable.get('run_default_preprocessing', default_var='True')):

    derived_tables_completed >> ensure_exclusive_db_flow_end
    send_mail_notification >> invalidate_cache

    slack_notification_task = PythonOperator(
        task_id="slack_notification_task",
        trigger_rule="all_done",
        python_callable=sl.slack__on_task_failure,
        provide_context=True,
    )

    create_warehouse_summary = PythonOperator(
        task_id="create_warehouse_summary",
        python_callable=ut.warehouse_summary,
        provide_context=True,
        trigger_rule="none_skipped",
    )

    if ingestion_config.get("check_if_run_master_table"):
        # store split logic starts

        with TaskGroup(group_id="store_split_group") as store_split:

            find_hierarchy_count_task = PythonOperator(
                task_id="find_the_hierarchy_count",
                python_callable=find_hierarchy_count,
                op_kwargs={
                    "project": "{{var.value.get('gcp_project')}}",
                    "dataset": "{{var.value.get('dataset')}}",
                },
            )

            sp_t1 = PythonOperator(
                task_id="temp_base_set_query",
                python_callable=StoreSplitClass(
                    ingestion_config.get("gcp_project"),
                    ingestion_config.get("dataset"),
                    ingestion_config.get("hierarchy_count", 0),
                ).create_base,
            )

            create_base_task = GenericDataWarehouseOperator(
                    task_id="create_temp_base",
                    sql="{{var.value.get('base_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="{{var.value.get('base_table', '')}}",
                    write_disposition="WRITE_TRUNCATE"
                )
            
            sp_t2 = PythonOperator(
                task_id="temp_txn_8weeks_query",
                python_callable=StoreSplitClass(
                    ingestion_config.get("gcp_project"),
                    ingestion_config.get("dataset"),
                    ingestion_config.get("hierarchy_count", 0),
                ).create_txn_8weeks,
            )

            create_txn_8weeks_task = GenericDataWarehouseOperator(
                    task_id="create_temp_txn_8weeks",
                    sql="{{var.value.get('txn_8weeks_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="{{var.value.get('txn_8weeks_table', '')}}",
                    write_disposition="WRITE_TRUNCATE"
                )
            

            sp_t3 = PythonOperator(
                task_id="temp_inventory_data_set_query",
                python_callable=StoreSplitClass(
                    ingestion_config.get("gcp_project"),
                    ingestion_config.get("dataset"),
                    ingestion_config.get("hierarchy_count", 0),
                ).create_inventory_data,
            )

            create_inventory_data_task = GenericDataWarehouseOperator(
                    task_id="create_temp_inventory_data",
                    sql="{{var.value.get('inventory_data_query, '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="{{var.value.get('inventory_data_table', '')}}",
                    write_disposition="WRITE_TRUNCATE"
                )

            sp_t4 = PythonOperator(
                task_id="temp_base_data_usual_set_query",
                python_callable=StoreSplitClass(
                    ingestion_config.get("gcp_project"),
                    ingestion_config.get("dataset"),
                    ingestion_config.get("hierarchy_count", 0),
                ).create_base_data_usual,
            )


            create_base_data_usual_task = GenericDataWarehouseOperator(
                    task_id="create_temp_base_data_usual",
                    sql="{{var.value.get('base_data_usual_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="{{var.value.get('base_data_usual_table', '')}}",
                    write_disposition="WRITE_TRUNCATE"
                )

            sp_t5 = PythonOperator(
                task_id="temp_proportions_set_query",
                python_callable=StoreSplitClass(
                    ingestion_config.get("gcp_project"),
                    ingestion_config.get("dataset"),
                    ingestion_config.get("hierarchy_count", 0),
                ).create_proportions,
            )

            create_proportions_task = GenericDataWarehouseOperator(
                    task_id="create_temp_proportions",
                    sql="{{var.value.get(proportions_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="{{var.value.get('proportions_table', '')}}",
                    write_disposition="WRITE_TRUNCATE"
                )
            

            sp_t6 = PythonOperator(
                task_id="temp_existing_products_set_query",
                python_callable=StoreSplitClass(
                    ingestion_config.get("gcp_project"),
                    ingestion_config.get("dataset"),
                    ingestion_config.get("hierarchy_count", 0),
                ).create_existing_products,
            )

            
            create_existing_products_task = GenericDataWarehouseOperator(
                    task_id="create_temp_existing_products",
                    sql="{{var.value.get(existing_products_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="{{var.value.get('existing_products_table', '')}}",
                    write_disposition="WRITE_TRUNCATE"
                )

            sp_t7 = PythonOperator(
                task_id="final_table_set_query",
                python_callable=StoreSplitClass(
                    ingestion_config.get("gcp_project"),
                    ingestion_config.get("dataset"),
                    ingestion_config.get("hierarchy_count", 0),
                ).create_final_table,
            )


            
            create_final_table_task = GenericDataWarehouseOperator(
                    task_id="create_final_table",
                    sql="{{var.value.get('final_table_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="{{var.value.get('final_table', '')}}",
                    write_disposition="WRITE_TRUNCATE"
                )
            
            (
                find_hierarchy_count_task
                >> sp_t1
                >> create_base_task
                >> sp_t2
                >> create_txn_8weeks_task
                >> sp_t3
                >> create_inventory_data_task
                >> sp_t4
                >> create_base_data_usual_task
                >> sp_t5
                >> create_proportions_task
                >> sp_t6
                >> create_existing_products_task
                >> sp_t7
                >> create_final_table_task
            )

        # day_split starts
        with TaskGroup(group_id="day_split_group") as day_split:

            dp_t1 = PythonOperator(
                task_id="create_query_temp_table",
                python_callable=DaySplit(
                    "{{var.value.get('gcp_project')}}",
                    "{{var.value.get('dataset')}}",
                    "{{var.value.get('modelling_level', '')}}",
                    "{{var.value.get('day_split_level', '')}}",
                ).create_temp_table_on_bq,
            )

         
            
            dp_t2 = GenericDataWarehouseOperator(
                    task_id=f"create_temp_table_{ingestion_config.get('day_split_level', '')}",
                    sql="{{var.value.get('day_split_temp_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="day_split_temporary",
                    write_disposition="WRITE_TRUNCATE"
                )

            dp_t3 = PythonOperator(
                task_id="create_query_day_split_table",
                python_callable=DaySplit(
                    "{{var.value.get('gcp_project')}}",
                    "{{var.value.get('dataset')}}",
                    "{{var.value.get('modelling_level', '')}}",
                    "{{var.value.get('day_split_level', '')}}",
                ).create_day_split_on_bq,
            )

            dp_t4 = GenericDataWarehouseOperator(
                    task_id=f"create_day_split_table_{ingestion_config.get('day_split_level', '')}",
                    sql="{{var.value.get('day_split_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                    destination_dataset_table="day_split_table",
                    write_disposition="WRITE_TRUNCATE"
                )

            (dp_t1 >> dp_t2 >> dp_t3 >> dp_t4)

        master_query = PythonOperator(
            task_id="generate_the_master_query", python_callable=generate_query
        )

        master_validate_table = GenericDataWarehouseOperator(
                    task_id="create_master_validated_table",
                    sql="{{var.value.get('master_validated_table_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                )

        run_master_validated_qc = PythonOperator(
            task_id="run_qc_on_master_validated_table",
            python_callable=on_success_qc_callback,
            op_kwargs={
                "tab_name": "master_validated_table",
                "entity_name": "master_validated",
                "tab_location":"""{{ var.value.get("warehouse") }}""",
                "mod_name": "preprocessing",
            },
        )

        master_table = GenericDataWarehouseOperator(
                    task_id="create_master_table",
                    sql="{{var.value.get('master_query', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                )

        run_master_table_qc = PythonOperator(
            task_id="run_qc_on_master_table",
            python_callable=on_success_qc_callback,
            op_kwargs={
                "tab_name": "master_table",
                "entity_name": "master_table",
                "tab_location":"""{{ var.value.get("warehouse") }}""",
                "mod_name": "preprocessing",
            },
        )

        task_master = (
            fiscal_task
            >> master_query
            >> master_validate_table
            >> run_master_validated_qc
            >> master_table
            >> run_master_table_qc
        )

        # if lsi needs to be skipped and keep cluster imputed table same as the master table
        if ingestion_config.get("run_lsi_flag") == False:

            if (ingestion_config.get("ingestion_type") == "non-periodic") or (
                ingestion_config.get("main_master_schema_change") == True
            ):

                # if historic run or if there is an update in master table schema, a materialized view of cluster imputed table is created
                t2_2 = PythonOperator(
                    task_id="Generate_LSI_query",
                    python_callable=lsi.lost_sales_imputation,
                    trigger_rule="none_failed",
                )

                t2_4 = GenericDataWarehouseOperator(
                    task_id="create_cluster_imputation_table",
                    sql="{{var.value.get('lsi_query_mv', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                )

                lsi_tasks = task_master >> t2_2 >> t2_4
            else:
                # if periodic, materialized view of cluster imputed table will be refreshed automatically
                t2_4 = DummyOperator(
                    task_id="auto_updation_of_imputation_table",
                    trigger_rule="all_success",
                    on_success_callback=sl.slack__on_task_success,
                )
                lsi_tasks = task_master >> t2_4

            master_end_task = DummyOperator(
                task_id="lsi_skipped", trigger_rule="all_success"
            )
            master_tasks = lsi_tasks >> master_end_task

        elif eval(Variable.get("run_lsi_flag", default_var="True")) == True:

            if eval(Variable.get("run_store_clustering", default_var="True")) == True:
                run_store_clustering = PythonOperator(
                    task_id="run_store_clustering",
                    python_callable=store_clustering.store_clustering,
                )

                sister_store_mapping = PythonOperator(
                    task_id="sister_store_mapping",
                    python_callable=store_clustering.sister_store_mapping,
                )

                clustering_branch = BranchPythonOperator(
                    task_id="clustering_branch",
                    python_callable=store_clustering.clustering_branch,
                )
            else:
                run_store_clustering = DummyOperator(
                    task_id="Dummy_run_store_clustering", trigger_rule="all_success"
                )

                sister_store_mapping = DummyOperator(
                    task_id="Dummy_sister_store_mapping", trigger_rule="all_success"
                )

                clustering_branch = DummyOperator(
                    task_id="clustering_branch", trigger_rule="all_success"
                )

            t2_2 = PythonOperator(
                task_id="Generate_LSI_query",
                python_callable=lsi.lost_sales_imputation,
                trigger_rule="none_failed",
            )

            t2_3 = GenericDataWarehouseOperator(
                    task_id="create_cluster_imputation_validated_table",
                    sql="{{var.value.get('lsi_query_validated', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                )
            

            run_master_imputed_validated_qc = PythonOperator(
                task_id="run_qc_on_master_imputed_validated_table",
                python_callable=on_success_qc_callback,
                op_kwargs={
                    "tab_name": "master_input_imputed_validated_table",
                    "entity_name": "master_input_imputed",
                    "tab_location":"""{{ var.value.get("warehouse") }}""",
                    "mod_name": "preprocessing",
                },
            )

            t2_4 = GenericDataWarehouseOperator(
                    task_id="create_cluster_imputation_table",
                    sql="{{var.value.get('lsi_query_main', '')}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                )
            
            run_master_imputed_qc = PythonOperator(
                task_id="run_qc_on_master_imputed_table",
                python_callable=on_success_qc_callback,
                op_kwargs={
                    "tab_name": "master_input_imputed",
                    "entity_name": "master_input_imputed",
                    "tab_location":"""{{ var.value.get("warehouse") }}""",
                    "mod_name": "preprocessing",
                },
            )

            master_end_task = DummyOperator(
                task_id="lsi_completed",
                trigger_rule="all_success",
                on_success_callback=sl.slack__on_task_success,
            )

            fiscal_task = run_master_imputed_qc
            master_tasks = (
                task_master
                >> clustering_branch
                >> [run_store_clustering, sister_store_mapping]
                >> t2_2
                >> t2_3
                >> run_master_imputed_validated_qc
                >> t2_4
                >> fiscal_task
                >> master_end_task
            )

        (master_end_task >> store_split >> create_warehouse_summary)
        (master_end_task >> day_split >> create_warehouse_summary)

    else:
        master_tasks = DummyOperator(
            task_id="pass_main_master_updation", trigger_rule="all_success"
        )
        store_split = DummyOperator(
            task_id="pass_store_split_updation", trigger_rule="all_success"
        )
        day_split = DummyOperator(
            task_id="pass_day_split_updation", trigger_rule="all_success"
        )
        (master_tasks >> store_split >> create_warehouse_summary)
        (master_tasks >> day_split >> create_warehouse_summary)

    if ingestion_config.get("check_if_run_fmt", "False"):

        task_find_aggregation_tables = PythonOperator(
            task_id="find_all_the_aggregation_table_names",
            python_callable=AggregateTable().find_table_names,
        )

        if ingestion_config.get("model_refresh", "False"):
            create_product_master_snapshot = BigQueryOperator(
                task_id="create_product_master_snapshot",
                gcp_conn_id="warehouse_connection",
                use_legacy_sql=False,
                sql="""
                    create or replace table {{var.value.get('gcp_project')}}.{{var.value.get('dataset')}}.product_master_modeling_snapshot as 
                    (select * from {{var.value.get('gcp_project')}}.{{var.value.get('dataset')}}.product_master) 
                """,
                
            )
        else:
            create_product_master_snapshot = DummyOperator(
                task_id="pass_create_product_master_snapshot",
                trigger_rule="all_success",
            )

        modeling_hierarchy_ingestion_flag = (
            "non-periodic"
            if ingestion_config.get("ingestion_type", "periodic") == "non-periodic"
            or ingestion_config.get("model_refresh", False)
            else "periodic"
        )
        run_hierarchy_code_for_modelling = PostgresOperator(
            # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
            task_id="build_hierarchy_code_for_modelling",
            sql=f"""call global.build_product_hierarchies_filter_for_modelling(
                    array{{{{var.value.get('hierarchy_code_for_modelling_level', '[]')}}}}::text[], 
                    '{modeling_hierarchy_ingestion_flag}'
                    );""",
        )

        copy_modeling_hierarchy_table_gbq = DbtoWareHouseOperatorSelector(
            task_id="copy_product_modeling_hierarchy_filter_flattened_from_db_to_warehouse",
            db_table="product_hierarchy_codes_for_modelling",
            db_type="postgresql",
            replace_table="true",
            data_flow_job_name=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-copy-product-modeling-hierarchies-filter-flattened-psg-warehouse",
            warehouse_table="product_hierarchy_codes_for_modelling",
            num_workers=1,
            max_num_workers=5,
            ingestion_config=ingestion_config,
        )

        create_product_hierarchies_ada = BigQueryOperator(
            task_id="create_product_hierarchies_ada",
            gcp_conn_id="warehouse_connection",
            use_legacy_sql=False,
            sql="""
                CREATE OR REPLACE TABLE {{var.value.get("gcp_project")}}.{{var.value.get("dataset")}}.product_hierarchies_ada AS 
                (
                    SELECT * 
                    FROM {{var.value.get("gcp_project")}}.{{var.value.get("dataset")}}.product_hierarchy_codes_for_modelling
                    where level in (
                        select max(level) 
                        FROM {{var.value.get("gcp_project")}}.{{var.value.get("dataset")}}.product_hierarchy_codes_for_modelling
                    )
                )
            """,
            
        )

        aggregation_tasks = []
        for ft, fl, fub, fjs, fjc in zip(
            eval(Variable.get("fmt_tags", default_var='["dummy"]')),
            eval(Variable.get("fmt_level", default_var="[1]")),
            eval(Variable.get("fmt_unique_by", default_var='["dummy"]')),
            eval(Variable.get("fmt_join_str", default_var='["dummy"]')),
            eval(Variable.get("fmt_join_condition", default_var='["dummy"]')),
        ):

            with TaskGroup(group_id=f"agg_{ft}") as agg_tasks:

                # Dummy operator to denote a starting point
                start_aggregation = DummyOperator(
                    task_id=f"start_aggregating_{ft}", trigger_rule="all_success"
                )

                task_create_agg_query = PythonOperator(
                    task_id=f"generate_the_aggregate_query_{ft}",
                    python_callable=AggregateTable().get_aggregate_query,
                    op_kwargs={
                        "table": ft,
                        "num": fl,
                    },
                )
              

                task_create_agg_validated_table_gbq = GenericDataWarehouseOperator(
                    task_id=f"create_aggregate_validated_table_{ft}",
                    sql=f"{{{{var.value.get('aggregation_validated_query_{ft}_{fl}', '')}}}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                )

                run_qc_on_aggregate_validated_table_qc = PythonOperator(
                    task_id=f"run_qc_on_aggregate_validated_table_{ft}",
                    python_callable=on_success_qc_callback,
                    op_kwargs={
                        "tab_name": f"{ft}_validated_table",
                        "entity_name": ft,
                        "tab_location":"""{{ var.value.get("warehouse") }}""",
                        "mod_name": "preprocessing",
                    },
                )


                task_create_agg_table_gbq = GenericDataWarehouseOperator(
                    task_id=f"create_aggregate_table_{ft}",
                    sql=f"{{{{var.value.get('aggregation_query_{ft}_{fl}', '')}}}}",
                    conn_id="warehouse_connection",
                    warehouse="""{{ var.value.get("warehouse") }}""",
                )


                run_qc_on_aggregate_table_qc = PythonOperator(
                    task_id=f"run_qc_on_aggregate_table_{ft}",
                    python_callable=on_success_qc_callback,
                    op_kwargs={
                        "tab_name": f"{ft}",
                        "entity_name": ft,
                        "tab_location":"""{{ var.value.get("warehouse") }}""",
                        "mod_name": "preprocessing",
                    },
                )

                jb_name = fl
                warehouse_table_for_di_df_copy_aggreagated_table = (
                    f"{ft}_validated_table"
                    if (
                        ingestion_config.get("ingestion_type") == "periodic"
                        and not ingestion_config.get("fmt_schema_change")
                    )
                    else ft
                )
                copy_agg_table = WareHouseToDbOperatorSelector(
                    task_id=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-aggreagated-table-warehouse-to-psg-{jb_name}",
                    db_type="postgresql",
                    db_schema_name="public",
                    data_flow_job_name=f"{ingestion_config.get('tenant_alias')}-{ingestion_config.get('pipeline')}-di-df-copy-aggreagated-table-warehouse-to-psg-{jb_name}",
                    single_table_config={
                        "warehouse_table": warehouse_table_for_di_df_copy_aggreagated_table,
                        "db_table": f"{ft}_validated",
                        "replace_table": "true",
                    },
                    num_workers=1,
                    max_num_workers=7,
                    ingestion_config=ingestion_config,
                )

                if (
                    ingestion_config.get("ingestion_type") != "periodic"
                    or ingestion_config.get("fmt_schema_change")
                    or ingestion_config.get("model_refresh", "False")
                ):

                    skip_reclass_flag = eval(
                        "False"
                        if ingestion_config.get(
                            "hierarchy_code_for_modelling_level", "[]"
                        )
                        == "[]"
                        else "True"
                    ) or ingestion_config.get("reclass_flag", False)
                    create_aggregated_table_with_filter = PostgresOperator(
                        # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                        task_id=f"create_{ft}",
                        sql=f"""call public.create_fmt(
                                '{ft}',
                                {int(fl)},
                                '{fjs}',
                                {skip_reclass_flag}
                                ); 
                            """,
                    )
                else:

                    skip_reclass_flag = eval(
                        "False"
                        if ingestion_config.get(
                            "hierarchy_code_for_modelling_level", "[]"
                        )
                        == "[]"
                        else "True"
                    ) or eval(Variable.get("reclass_flag", default_var="False"))
                    create_aggregated_table_with_filter = PostgresOperator(
                        # postgres_conn_id="{{var.value.get('postgres_conn_id', 'postgres_default')}}",
                        task_id=f"update_{ft}",
                        sql=f"""
                            call public.update_fmt(
                                '{ft}',
                                {int(fl)},
                                '{fjs}',
                                {skip_reclass_flag}
                                );
                            """,
                    )
                end_aggregation = DummyOperator(
                    task_id=f"end_aggregating_{ft}",
                    trigger_rule="none_failed_min_one_success",
                )

                (
                    start_aggregation
                    >> task_create_agg_query
                    >> task_create_agg_validated_table_gbq
                    >> run_qc_on_aggregate_validated_table_qc
                    >> task_create_agg_table_gbq
                    >> run_qc_on_aggregate_table_qc
                    >> copy_agg_table
                    >> create_aggregated_table_with_filter
                    >> end_aggregation
                )
            aggregation_tasks.append(agg_tasks)

        (
            fiscal_task
            >> master_tasks
            >> task_find_aggregation_tables
            >> create_product_master_snapshot
            >> run_hierarchy_code_for_modelling
            >> copy_modeling_hierarchy_table_gbq
            >> create_product_hierarchies_ada
            >> aggregation_tasks
        )
    else:
        aggregation_tasks = DummyOperator(
            task_id="skipping_aggregation", trigger_rule="all_success"
        )
        (fiscal_task >> master_tasks >> aggregation_tasks)

    aggregation_tasks >> preprocessing_ends

    send_qc_mail_notification = PythonOperator(
        task_id="send_qc_mail_notification",
        python_callable=send_notification,
        op_kwargs={"notification_type": NotificationType.QC.value},
    )

    stop_ingestion_vm_instance = PythonOperator(
        task_id="stop_ingestion_vm_instance",
        python_callable=call_vm_stop_api,
        op_kwargs={
            "instance_name": "{{var.value.get('tenant_alias')}}-ingestion-{{var.value.get('pipeline')}}"
        },
    )

    check_long_running_task = PythonOperator(
        task_id="check_long_running_task",
        python_callable=ut.long_task_sensor,
        trigger_rule="always",
        op_kwargs={"timeout_hours": 10, "poke_interval": 360},
        provide_context=True,
    )

    cleanup_task = BashOperator(
        task_id="cleanup_old_logs",
        trigger_rule="all_done",
        bash_command="./cleanup_old_logs.sh",
        dag=dag,
    )

    (
        preprocessing_ends
        >> create_warehouse_summary
        >> cleanup_task
        >> slack_notification_task
        >> send_qc_mail_notification
        >> stop_ingestion_vm_instance
    )
