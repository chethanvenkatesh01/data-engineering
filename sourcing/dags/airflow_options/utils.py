import re
from typing import Any, List, Tuple

from dataflow_options.utils import Logger

import snowflake.connector
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import ResultMetadata
from database_utility.GenericDatabaseConnector import WarehouseConnector
from queries import (
    bigquery_queries as bq_q,
    snowflake_queries as sf_q,
    query_builder as qb,
)
from database_utility.utils import escape_column_name
from airflow.exceptions import AirflowException


log = Logger(__name__)


def TryExcept(func):
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            retval = func(*args, **kwargs)
        except Exception as e:
            log.error(
                f"Exception occured in {func.__name__}. Exception: {str(e)}",
                exc_info=True,
            )
            raise e
        return retval

    return wrapper


def idtoken_from_metadata_server(url: str):
    from google.auth import compute_engine
    from google.auth.transport.requests import Request

    request = Request()
    # the service_account_email.
    credentials = compute_engine.IDTokenCredentials(
        request=request, target_audience=url, use_metadata_identity_endpoint=True
    )
    token = credentials.refresh(request)
    return credentials.token


def read_query(query):
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    return wc.execute_query(query, False)


def is_deleted_setter_for_mapping():
    # for old clients is_deleted is not present
    from airflow.models import Variable

    where_clause = " "

    if Variable.get("view_mapping_table") != Variable.get("mapping_table"):
        where_clause = f" AND is_deleted = False "
    return where_clause


def fetch_param(**kwargs):
    from airflow.models import Variable
    from datetime import datetime, timedelta
    import os
    import json
    import pytz
    from croniter import croniter
    from collections import OrderedDict

    gcp_dataset = Variable.get("gcp_dataset")
    region = f"region-{Variable.get('bigquery_region', default_var='US')}"

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    view_mapping_table = wc._get_complete_table_name(
        Variable.get("view_mapping_table"), True
    )
    mapping_table = wc._get_complete_table_name(
        Variable.get("mapping_table"), True
    )

    # for old clients
    if view_mapping_table == mapping_table:
        from_clause = f""" 
            SELECT * 
            FROM {view_mapping_table}
            WHERE VIEW IS NOT NULL 
        """
    # for new clients
    else:
        from_clause = f""" SELECT v.*, i.dataingestion_filterparam,i.intermediate_table,i.inter_query_exec_order FROM {view_mapping_table} as v JOIN {mapping_table} as i ON v.{escape_column_name('table',warehouse)}=i.{escape_column_name('table',warehouse)} WHERE v.is_deleted=False and i.is_deleted = False and v.view IS NOT NULL"""

    query = qb.build_fetch_params_query(from_clause)

    df = wc.execute_query(query, True)

    df = df.reset_index()

    entities = []
    views = []
    source_formats = {}
    croniter_base = datetime.now(
        pytz.timezone(Variable.get("dag_timezone", default_var="UTC"))
    ) - timedelta(days=1)
    croniter_base = croniter_base.astimezone(
        pytz.timezone(Variable.get("dag_timezone", default_var="UTC"))
    )
    for idx, row in df.iterrows():
        iter = croniter(row["schedule_interval"], croniter_base)

        var_1 = datetime.now(
            pytz.timezone(Variable.get("dag_timezone", default_var="UTC"))
        ).date()
        var_2 = iter.get_next(datetime).date()
        log.info(f"var_1 : {var_1} , var_2 : {var_2}")
        if var_1 == var_2:
            # if True:
            entities.append(row["table"])
            views.append(row["view"])
            source_formats[row["view"]] = row["source_format_regex"]

    # kwargs['ti'].xcom_push(key="entities", value=entities)
    # kwargs['ti'].xcom_push(key="views", value=views)

    unique_views = list(set(views))
    overall_config = get_overall_config()
    overall_config["unique_views"] = unique_views
    Variable.set("unique_views", unique_views)
    # with open('/opt/airflow/dags/dynamic_dag_configs/unique_views.py', 'w') as file:
    #     file.write(f"UNIQUE_VIEWS = {unique_views}")

    unique_entities = []
    for entity in entities:
        if entity not in unique_entities:
            unique_entities.append(entity)
    # unique_entities = list(set(entities))

    overall_config["unique_entities"] = unique_entities
    Variable.set("overall_config", json.dumps(overall_config))
    Variable.set("entities", entities)
    Variable.set("views", views)
    if Variable.get("trigger_type", default_var="None") != "None":
        Variable.set(f"view_trigger_status_mapping", {view: False for view in views})
    else:
        Variable.set(f"view_trigger_status_mapping", {view: True for view in views})

    entity_view_dict = OrderedDict()
    # unique_entities = list(
    #     {value: None for value in eval(Variable.get("entities"))}.keys()
    # )
    Variable.set("unique_entites", unique_entities)
    # kwargs['ti'].xcom_push(key="unique_entities", value=unique_entities)
    Variable.set("row_count_validation", "False")
    for entity in unique_entities:
        param_df = df[df["table"] == entity].copy().reset_index()

        Variable.set(f"{entity}_views_count", len(param_df))
        entity_view_dict[entity] = param_df.view.to_list()

        for i in param_df.index:
            Variable.set(f"{entity}_param_view_{i}", param_df.loc[i, "view"])
            Variable.set(f"{entity}_param_filter_{i}", param_df.loc[i, "filter"])
            Variable.set(f"{entity}_param_replace_{i}", param_df.loc[i, "replace"])
            Variable.set(f"{entity}_param_pull_type_{i}", param_df.loc[i, "pull_type"])
            Variable.set(
                f"{entity}_param_exec_order", param_df.loc[i, "inter_query_exec_order"]
            )

            view = param_df.loc[i, "view"]
            Variable.set(
                f"{view}_param_source_config", param_df.loc[i, "source_config"]
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_source_config", value=param_df.loc[i, "source_config"])
            Variable.set(f"{view}_param_filter", param_df.loc[i, "filter"])
            # kwargs['ti'].xcom_push(key=f"{view}_param_filter", value=param_df.loc[i, "filter"])
            Variable.set(f"{view}_param_filter_param", param_df.loc[i, "filter_param"])
            # kwargs['ti'].xcom_push(key=f"{view}_param_filter_param", value=param_df.loc[i, "filter_param"])
            Variable.set(f"{view}_param_replace", param_df.loc[i, "replace"])
            # kwargs['ti'].xcom_push(key=f"{view}_param_replace", value=bool(param_df.loc[i, "replace"]))
            Variable.set(f"{view}_param_connector", param_df.loc[i, "connector"])
            # kwargs['ti'].xcom_push(key=f"{view}_param_connector", value=param_df.loc[i, "connector"])
            Variable.set(
                f"{view}_param_schedule_interval", param_df.loc[i, "schedule_interval"]
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_schedule_interval", value=param_df.loc[i, "schedule_interval"])
            Variable.set(f"{view}_param_pull_type", param_df.loc[i, "pull_type"])
            # kwargs['ti'].xcom_push(key=f"{view}_param_pull_type", value=param_df.loc[i, "pull_type"])
            Variable.set(
                f"{view}_param_partition_column", param_df.loc[i, "partition_column"]
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_partition_column", value=param_df.loc[i, "partition_column"])
            Variable.set(
                f"{view}_param_clustering_columns",
                (
                    param_df.loc[i, "clustering_columns"].split(",")
                    if param_df.loc[i, "clustering_columns"]
                    else None
                ),
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_clustering_columns", value=param_df.loc[i, "clustering_columns"].split(",") if param_df.loc[i, "clustering_columns"] else None)
            Variable.set(
                f"{view}_param_source_format_regex",
                param_df.loc[i, "source_format_regex"],
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_source_format_regex", value=param_df.loc[i, "source_format_regex"])
            Variable.set(
                f"{view}_param_field_delimiter", param_df.loc[i, "field_delimiter"]
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_field_delimiter", value=param_df.loc[i, "field_delimiter"])
            Variable.set(
                f"{view}_param_extraction_sync_dt",
                param_df.loc[i, "extraction_sync_dt"],
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_extraction_sync_dt", value=str(param_df.loc[i, "extraction_sync_dt"]))
            # Variable.set(f"{view}_param_source_query" ,param_df.loc[i, 'source_query'])
            Variable.set(
                f"{view}_param_num_rows_before_load_job", param_df.loc[i, "num_rows"]
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_num_rows_before_load_job", value=int(param_df.loc[i, "num_rows"]))
            Variable.set(
                f"{view}_param_replace_special_characters",
                param_df.loc[i, "replace_special_characters"],
            )
            # kwargs['ti'].xcom_push(key=f"{view}_param_replace_special_characters", value=bool(param_df.loc[i, "replace_special_characters"]))
            # set row count validation values
            if param_df.loc[i, "row_count_validation_info"]:
                Variable.set(
                    f"{view}_param_row_count_validation_info",
                    param_df.loc[i, "row_count_validation_info"],
                )
                # kwargs['ti'].xcom_push(key=f"{view}_param_row_count_validation_info", value=param_df.loc[i, "row_count_validation_info"])
                Variable.set("row_count_validation", "True")
                # kwargs['ti'].xcom_push(key=f"{view}row_count_validation", value="True")

            Variable.set(
                f"{view}_param_miscellaneous_attributes",
                param_df.loc[i, "miscellaneous_attributes"],
            )

    timezone = pytz.timezone(Variable.get("dag_timezone", default_var="UTC"))
    curr_datetime = datetime.now(timezone)
    curr_date = curr_datetime.date()
    trigger_cutoff_time = datetime.strptime(
        Variable.get("trigger_cutoff_time", default_var="00:00"), "%H:%M"
    ).time()
    trigger_cutoff_datetime = timezone.localize(
        datetime.combine(curr_date, trigger_cutoff_time)
    )
    if curr_datetime >= trigger_cutoff_datetime:
        Variable.set("trigger_timeout", 0)
    else:
        Variable.set(
            "trigger_timeout", (trigger_cutoff_datetime - curr_datetime).seconds
        )

    return None


def select_gcs_to_warehouse_operator(view, task_group_id):
    from airflow.models import Variable

    normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', view)

    if Variable.get(f"{view}_param_connector", "None").upper() == "SFTP":
        return f"{task_group_id}.sftp_pull_{normalized_view_name}_to_warehouse"
    elif Variable.get(f"{view}_param_connector", "None").upper() == "GCS":
        return f"{task_group_id}.gcs_pull_{normalized_view_name}_to_warehouse"
    elif Variable.get(f"{view}_param_connector", "None").upper() == "SNOWFLAKE":
        return f"{task_group_id}.snowflake_pull_{normalized_view_name}_to_warehouse"
    else:
        return f"{task_group_id}.db_pull_{normalized_view_name}_to_warehouse"


def download_blob_content(bucket_name, source_blob_name):
    """Downloads a single blob from GCS."""
    from google.cloud.storage import Client

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_string().decode("utf-8")


def extract_entity_from_blob_name(blob_name):
    """
    Blob name can either of the below :
    f"create_{entity}_historic.json or create_{entity}_historic.sql "
    f"create_{entity}.json or create_{entity}.sql"
    """
    import re

    suffixes = [".json", ".sql"]

    for suffix in suffixes:
        if blob_name.endswith(suffix):
            blob_name = blob_name[: -len(suffix)]

    # Define the regular expression pattern to match the entity
    pattern = r"create_(.*?)_historic|create_(.*)"
    # Search for matches using the regular expression
    match = re.search(pattern, blob_name)
    # If a match is found, return the entity
    if match:
        # Check which group has the match
        entity = match.group(1) if match.group(1) else match.group(2)
        return entity
    else:
        return None


def get_query_from_gcs():
    from airflow.models import Variable
    from google.cloud.storage import Client
    from concurrent.futures import as_completed, ThreadPoolExecutor

    client = Client()
    bucket_name = Variable.get("gcp_bucket")
    bucket = client.get_bucket(bucket_name)
    source_folder = Variable.get("intermediate_queries_loc")
    blobs = list(bucket.list_blobs(prefix=source_folder))

    log.info(f"bucket_name:  {bucket_name}")
    log.info(f" source_folder:  {source_folder}")
    log.info(f"blobs:  {blobs}")

    combined_json = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        # download_blob
        # Create a future to blob content mapping
        future_to_blob = {
            executor.submit(download_blob_content, bucket_name, blob.name): blob
            for blob in blobs
            if not blob.name.endswith("/")
        }

        # As each thread completes, get the result and add it to the JSON object
        for future in as_completed(future_to_blob):
            blob = future_to_blob[future]
            blob_name = blob.name
            try:
                content = future.result()
                file_name_without_extension = blob_name.split("/")[-1].split(".")[0]
                combined_json[file_name_without_extension] = content
            except Exception as exc:
                log.info(f"Blob {blob.name} generated an exception: {exc}")

    for entity in eval(Variable.get("entities")):
        if Variable.get("ingestion_type", default_var="historic") in (
            "historic",
            "non-periodic",
        ):
            Variable.set(
                f"{entity}_intermediate_query",
                combined_json[f"create_{entity}_historic"],
            )
        else:
            Variable.set(
                f"{entity}_intermediate_query",
                combined_json[f"create_{entity}"],
            )
    return None


def fetch_latest_sync_date(entities):
    from airflow.models import Variable
    from datetime import datetime
    import pytz
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from queries.query_builder import build_fetch_latest_date_query

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    view_name = wc._get_complete_table_name("view",True)
    for entity in entities:
        for i in range(eval(Variable.get(f"{entity}_views_count"))):
            view = Variable.get(f"{entity}_param_view_{i}")
            param_filter = Variable.get(f"{entity}_param_filter_{i}")
            replace = Variable.get(f"{entity}_param_replace_{i}")
            if replace == "false":
                query = build_fetch_latest_date_query(view_name, view, param_filter)
                result = wc.execute_query(query, True)
                if result.iloc[0, 0] is None:
                    Variable.set(
                        f"{entity}_latest_sync_date_{view}",
                        datetime.strftime(
                            datetime.now(
                                pytz.timezone(
                                    Variable.get("dag_timezone", default_var="UTC")
                                )
                            ),
                            format="%Y-%m-%d %H:%M:%S.%f",
                        ),
                    )
                else:
                    Variable.set(f"{entity}_latest_sync_date_{view}", result.iloc[0, 0])


def update_to_latest_syncdate_query(entities):
    from airflow.models import Variable

    """
    This function is not being called anywhere
    """
    project = Variable.get("gcp_project")
    dataset = Variable.get("gcp_dataset")
    mapping_table = Variable.get("mapping_table")
    query = f"""
        update {project}.{dataset}.{mapping_table}
        set filter_param =
        case view 
    """
    for entity in entities:
        for i in range(eval(Variable.get(f"{entity}_views_count"))):
            view = Variable.get(f"{entity}_param_view_{i}")
            replace = Variable.get(f"{entity}_param_replace_{i}")
            if replace == "false":
                date = Variable.get(f"{entity}_latest_sync_date_{view}")
                query += f" when '{view}' then cast('{date}' as datetime) "
                Variable.set(f"{entity}_incremental_view_present_flag", True)

    query += " else cast(filter_param as datetime)  end where true "
    Variable.set(f"update_latest_sync_date_query", query)


def get_file_size_gcs(entity, view):
    from airflow.models import Variable
    from google.cloud.storage import Client

    client = Client()
    bucket = client.get_bucket(Variable.get("gcp_bucket"))
    blobs = bucket.list_blobs(prefix=f"{entity}/{view}")
    blobs_size = []
    for blob in blobs:
        if blob.name.endswith("csv"):
            blobs_size.append(blob.size)
    return sum(blobs_size)


def check_csv_files_size(view):
    from google.cloud.storage import Client

    client = Client()
    bucket = client.get_bucket("mtp-dataflow3")
    blobs = bucket.list_blobs(prefix=f"/{view}/{view}")
    blobs_size = []
    for blob in blobs:
        if blob.name.endswith("csv"):
            blobs_size.append(blob.size)
    if sum(blobs_size) == 0:
        return [f"end_{view}"]
    else:
        return [f"pull_{view}_to_gbq", f"end_{view}"]


# call the data ingestion dag


def make_composer2_web_server_request(url: str, method: str = "GET", **kwargs: Any):
    import os
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    airflow_username = os.getenv("airflow_username", "airflow")
    airflow_password = os.getenv("airflow_password", "airflow")

    AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
    CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

    authed_session = AuthorizedSession(CREDENTIALS)

    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(
        method, url, auth=(airflow_username, airflow_password), **kwargs
    )


def trigger_dag(web_server_url: str, dag_id: str, data: dict) -> str:
    import requests

    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}
    log.info(f"endpoint : {endpoint}")
    log.info(f"request_url : {request_url}")
    log.info(f"json_data : {json_data}")
    response = make_composer2_web_server_request(
        request_url, method="POST", json=json_data
    )

    if response.status_code == 403:
        raise requests.HTTPError(
            "You do not have a permission to perform this operation. "
            "Check Airflow RBAC roles for your account."
            f"{response.headers} / {response.text}"
        )
    elif response.status_code != 200:
        response.raise_for_status()
    else:
        return response.text


def set_variable_through_task(view):
    from airflow.models import Variable

    Variable.set(f"{view}_load_task_status", 100)


def get_trigger_status(
    view, connector, config, trigger_rule, trigger_query, trigger_file
):
    if trigger_rule.upper() == "QUERY":
        result = execute_trigger_query(trigger_query, config, connector)
        return result
    elif trigger_rule.upper() == "FILE":
        result = check_trigger_file(trigger_file, config, connector, view)
        return result
    else:
        raise Exception(f"trigger_rule={trigger_rule} is not supported")


def check_trigger_file(trigger_file, config, connector, view):
    from airflow.models import Variable
    from datetime import datetime, timedelta
    import re
    import pytz
    import socket
    import pandas as pd
    from croniter import croniter
    from ssh2.session import Session
    from google.cloud.storage import Client
    from ssh2.exceptions import SFTPHandleError
    from ssh2.exceptions import SFTPProtocolError
    from ssh2.sftp import LIBSSH2_FXF_READ, LIBSSH2_SFTP_S_IRUSR

    py_date_format_mapping = {
        "yyyy-MM-dd": "%Y-%m-%d",
        "yyyyMMdd": "%Y%m%d",
        "yyMMdd": "%y%m%d",
    }
    view_trigger_mandatory_mapping: dict = eval(
        Variable.get("view_trigger_mandatory_mapping", default_var="{}")
    )
    if connector.lower() == "sftp":
        view_extraction_sync_dt = Variable.get(f"{view}_param_extraction_sync_dt")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((config["host"], int(config.get("port", 22))))
        session = Session()
        session.handshake(sock)
        session.userauth_password(config["user"], config["password"])
        sftp = session.sftp_init()
        log.info(f"{view} - SFTP Channel Created")
        parent_dir = config["path"] + ("" if config["path"].endswith("/") else "/")
        # date_based_folder_pattern:bool = False
        file_pattern = config.get(
            "file_pattern",
            "yyyy-MM-dd/{view}_\d+.*.(csv|dat|txt|csv.gz|dat.gz|txt.gz|parquet)",
        )
        date_match = re.search("(yyyy-MM-dd|yyyyMMdd|yyMMdd)", file_pattern)
        date_pattern = "yyyy-MM-dd"
        if date_match:
            date_pattern = date_match.group(0)
        else:
            log.warn(
                f"Date pattern (yyyy-MM-dd|yyyyMMdd|yyMMdd) not found in {file_pattern}"
            )
        Variable.set(f"{view}_folder_date_pattern", date_pattern)
        date_pattern = py_date_format_mapping.get(date_pattern, "%Y-%m-%d")
        log.info(f"Date Pattern: {date_pattern}")
        Variable.set(f"{view}_folder_date_pattern_py", date_pattern)
        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        log.info(f"{view} - Root directory {parent_dir}")
        # sftp_file_handle = sftp.opendir(parent_dir)
        last_date = (
            datetime.now(pytz.timezone(Variable.get("dag_timezone", default_var="UTC")))
            - timedelta(days=1)
        ).strftime("%Y%m%d")
        today = datetime.now(
            pytz.timezone(Variable.get("dag_timezone", default_var="UTC"))
        ).strftime("%Y%m%d")
        date_match = re.search("\d{4}-\d{2}-\d{1,2}", str(view_extraction_sync_dt))
        if date_match:
            last_date = date_match.group(0).replace("-", "")
        log.info(f"{view} - Last folder date: {last_date}")
        croniter_base = pytz.timezone(
            Variable.get("dag_timezone", default_var="UTC")
        ).localize(
            datetime.fromisoformat(Variable.get(f"{view}_param_extraction_sync_dt"))
        )
        cron_iter = croniter(
            Variable.get(f"{view}_param_schedule_interval"), croniter_base
        )
        start_offset = cron_iter.get_next(datetime).strftime("%Y-%m-%d")
        date_ranges = pd.date_range(
            start=start_offset,
            end=datetime.now(
                pytz.timezone(Variable.get("dag_timezone", default_var="UTC"))
            ).strftime("%Y-%m-%d"),
        )
        file_name_pattern = file_pattern.format(view=view).split("/")[-1]
        file_name_prefix = file_name_pattern.split(".")[0]
        Variable.set(f"{view}_file_prefix", f"{file_name_prefix}")
        Variable.set(
            f"{view}_file_suffix", "(csv|dat|txt|csv.gz|dat.gz|txt.gz|gz|parquet)"
        )
        dirs_with_trigger_file = []
        dirs_without_trigger_file = []
        for dt in date_ranges:
            base_path = (
                f"{parent_dir.rstrip('/')}/{dt.strftime(date_pattern)}/{view}"
                if file_pattern.count("{VIEW}") > 1
                else f"{parent_dir.rstrip('/')}/{dt.strftime(date_pattern)}"
            )
            trigger_file_path = f"{base_path}/{trigger_file}"
            try:
                log.info(f"Checking for trigger file {trigger_file_path}")
                sftp_file_handle = sftp.open(
                    trigger_file_path,
                    LIBSSH2_FXF_READ,
                    LIBSSH2_SFTP_S_IRUSR,
                )
                sftp_file_handle.close()
            except (SFTPHandleError, SFTPProtocolError):
                dirs_without_trigger_file.append(base_path)
                log.error(f"Cannot open file {trigger_file_path}")
                # Stop checking if the trigger is missing from earlier date folders in case of mandatory view
                if view_trigger_mandatory_mapping.get(view, False):
                    break
            except Exception as e:
                raise e
            else:
                dirs_with_trigger_file.append(base_path)
        return sorted(dirs_without_trigger_file), sorted(dirs_with_trigger_file)

    elif connector.lower() == "gcs":
        gcs_client = Client()
        file_pattern: str = config.get(
            "file_pattern",
            "yyyyMMdd/{view}/*.(csv|dat|txt|csv.gz|dat.gz|txt.gz|parquet)",
        )
        log.info(f"File Pattern: {file_pattern}")
        date_match = re.search("(yyyy-MM-dd|yyyyMMdd|yyMMdd)", file_pattern)
        date_pattern = "yyyyMMdd"
        if date_match:
            date_pattern = date_match.group(0)
        else:
            log.warn(
                f"Date match not found. Using the default date_pattern {date_pattern}"
            )
            # log.warn(f"Date pattern (yyyy-MM-dd|yyyyMMdd|yyMMdd) not found in {file_pattern}")
        Variable.set(f"{view}_folder_date_pattern", date_pattern)
        date_pattern = py_date_format_mapping.get(date_pattern, "%Y-%m-%d")
        log.info(f"Date Pattern: {date_pattern}")
        Variable.set(f"{view}_folder_date_pattern_py", date_pattern)
        croniter_base = pytz.timezone(
            Variable.get("dag_timezone", default_var="UTC")
        ).localize(
            datetime.fromisoformat(Variable.get(f"{view}_param_extraction_sync_dt"))
        )
        cron_iter = croniter(
            Variable.get(f"{view}_param_schedule_interval"), croniter_base
        )
        start_offset = cron_iter.get_next(datetime).strftime("%Y-%m-%d")
        date_ranges = pd.date_range(
            start=start_offset,
            end=datetime.now(
                pytz.timezone(Variable.get("dag_timezone", default_var="UTC"))
            ).strftime("%Y-%m-%d"),
        )
        bucket = gcs_client.bucket(config["bucket"])
        blob_prefix = config.get("blob_prefix", "")
        blob_prefix = blob_prefix + "/" if len(blob_prefix) > 0 else ""
        trigger_file_found = []
        dirs_without_trigger_file = []
        dirs_with_trigger_file = []
        if re.search("{view}/", file_pattern):
            # trigger_file_path = "{blob_prefix}/{date_pattern}/{view}"
            Variable.set(f"{view}_file_prefix", f"{view}/")
        else:
            # trigger_file_path = "{blob_prefix}/{date_pattern}"
            Variable.set(f"{view}_file_prefix", f"{view}")
        Variable.set(
            f"{view}_file_suffix", "(csv|dat|txt|csv.gz|dat.gz|txt.gz|gz|parquet)"
        )
        for dt in date_ranges:
            trigger_file_path = "{blob_prefix}{date_pattern}".format(
                blob_prefix=blob_prefix,
                date_pattern=dt.strftime(date_pattern),
                view=view,
            )
            if bucket.blob(f"{trigger_file_path}/{trigger_file}").exists():
                trigger_file_found.append(True)
                dirs_with_trigger_file.append(
                    f"gs://{config['bucket']}/{trigger_file_path}/"
                )
            else:
                trigger_file_found.append(False)
                dirs_without_trigger_file.append(
                    f"gs://{config['bucket']}/{trigger_file_path}/{trigger_file}"
                )
        if all(trigger_file_found):
            return [], sorted(dirs_with_trigger_file)
        else:
            return sorted(dirs_without_trigger_file), sorted(dirs_with_trigger_file)
    else:
        raise Exception(f"connector={connector} is not supported in triggers")


def execute_trigger_query(query: str, config: dict, connector: str) -> List[Tuple]:
    from sqlalchemy import create_engine

    if connector.lower() == "snowflake":
        username = config.get("username", None) or config.get("user", None)
        password = config.get("password", None)
        account = config.get("account", None)
        warehouse = config.get("warehouse", None)
        database = config.get("database", None)
        schema = config.get("schema", None)
        role = config.get("role", None)

        snowflake_conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
        )

        result = snowflake_conn.cursor().execute(query).fetchall()
        return result

    elif connector.lower() in [
        "postgresql",
        "postgres",
        "mssql",
        "sqlserver",
        "sql server",
        "mysql",
        "oracle",
    ]:
        url_obj = get_connection_url_from_config_value(config=config, db_type=connector)
        engine = create_engine(url_obj)
        result = None
        with engine.connect() as conn:
            result = conn.execute(query).fetchall()
        return result


def get_source_config_value(source_config_name: str) -> dict:
    from airflow.models import Variable
    from google.cloud import secretmanager

    secret_mgr_client = secretmanager.SecretManagerServiceClient()
    response = secret_mgr_client.access_secret_version(
        name=f"projects/{Variable.get('gcp_project')}/secrets/{source_config_name}/versions/latest"
    )
    config: dict = eval(response.payload.data.decode("UTF-8"))
    return config


def get_connection_url_from_config_value(config: dict, db_type: str = "mssql"):
    from sqlalchemy.engine.url import URL

    if "server" in config:
        host = config["server"]
    else:
        host = config["host"]
    user = config["user"]
    password = config["password"]
    database = config["database"]
    if db_type in ["mssql"]:
        port = config.get("port", "1433")
        return URL(
            f"mssql+pymssql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    elif db_type in ["postgresql", "postgres"]:
        port = config.get("port", "5432")
        return URL(
            f"postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    elif db_type in ["mysql"]:
        port = config.get("port", "3306")
        return URL(
            f"mysql+pymysql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    elif db_type in ["oracle"]:
        port = config.get("port", "1521")
        return URL(
            f"oracle+cx_oracle",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    else:
        raise Exception(f"DB Type {db_type} is not supported in this version")


@TryExcept
def update_filter_param(entity):
    from airflow.models import Variable
    from google.cloud import bigquery

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    view_mapping_table = wc._get_complete_table_name(
        Variable.get("view_mapping_table"), True
    )

    query = qb.build_update_param_col_details_query(entity)

    df = wc.execute_query(query, True)

    is_delete_flag = is_deleted_setter_for_mapping()

    update_query = qb.build_update_filter_param_query(
        entity, df, is_delete_flag, view_mapping_table
    )

    wc.execute_query(update_query, False)


@TryExcept
def update_extraction_sync_dt(view):
    from airflow.models import Variable
    from google.cloud import bigquery
    from database_utility.utils import DATA_TYPE_MAPPINGS

    audit_column_name = str(
        Variable.get(f"{view}_param_filter", default_var="SYNCSTARTDATETIME")
    ).upper()

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', view)
    view_mapping_table = wc._get_complete_table_name(
        Variable.get("view_mapping_table"), True
    )

    view_table = wc._get_complete_table_name(view, True)

    log.info(f"Audit Column Name {audit_column_name}")

    query = qb.get_col_for_update_extraction_sync_date_query(view)
    df = wc.execute_query(query, True)

    num_rows_after_load_job = wc._get_row_count(normalized_view_name)
    # If there are zero rows for full load
    # If there are zero rows for full load or no rows are no loaded for incremental
    pull_type = Variable.get(f"{view}_param_pull_type", default_var="incremental")
    num_rows_before_load_job = int(Variable.get(f"{view}_param_num_rows_before_load_job"))
    if (pull_type.lower() == "incremental" and num_rows_after_load_job <= num_rows_before_load_job) \
        or (pull_type.lower() == "full" and num_rows_after_load_job == 0):
        log.info(f"Zero rows loaded in the current run. So not updating the extraction_sync_dt")
        return

    is_delete_flag = is_deleted_setter_for_mapping()

    query_job = qb.build_update_query_for_update_extraction_sync_date(
        view_mapping_table, view, view_table, is_delete_flag, df
    )
    log.info(f"update_extraction_sync_dt job id: {query_job}")
    query_status = wc.execute_query(query_job, False)
    if query_status is None:
        raise AirflowException("update_extraction_sync_dt job failed")


def get_row_count_for_views():
    from airflow.models import Variable
    from airflow_options.gcp_utils import BigQuery
    import re
    views = eval(Variable.get("unique_views", default_var="[]"))
    # views_str = ",".join([f"'{vw}'" for vw in views])
    # query = f"""
    # SELECT table_name, total_rows AS num_rows
    # FROM `region-{Variable.get('bigquery_region', default_var='US')}`.INFORMATION_SCHEMA.TABLE_STORAGE
    # WHERE table_schema='{Variable.get('gcp_dataset')}'
    # AND table_name IN ({views_str})
    # """
    # log.info(f"Fetching the row count for all views \n{query}")
    # df = read_query(query)
    # df = df.set_index('table_name')
    # return df.to_dict('index')
    row_count = {}
    bq_client = BigQuery(
        project_id=str(Variable.get("gcp_project")),
        location=Variable.get("bigquery_region", default_var="US"),
    )
    for view in views:
        row_count[view] = bq_client.get_num_rows(f"{Variable.get('gcp_dataset')}", re.sub('[^A-Za-z0-9_]', '_', view))
        Variable.set(f"{view}_param_num_rows_after_load_job", row_count[view])
    return row_count


def get_trigger_mapping_from_db():
    from airflow.models import Variable
    from airflow_options.database_utils import connect_to_db_and_execute_query

    query = """
        select * from data_platform.generic_trigger_mapping where not is_deleted
    """
    db_host = Variable.get("PGXHOST")  # os.getenv("PGXHOST")
    db_port = Variable.get("PGXPORT")  # os.getenv("PGXPORT")
    db_database = Variable.get("PGXDATABASE")  # os.getenv("PGXDATABASE")
    db_user = Variable.get("PGXUSER")  # os.getenv("PGX_USER")
    db_password = Variable.get("PGXPASSWORD")  # os.getenv("PGXPASSWORD")
    trigger_df = connect_to_db_and_execute_query(
        db_host, db_port, db_database, db_user, db_password, query
    )
    return trigger_df


def get_overall_config():
    from airflow.models import Variable

    overall_config = Variable.get(
        "overall_config",
        default_var={
            "sourcing_config": {
                "dag_configs": {
                    "dag_generic_schedule_time": None,
                    "dag_timezone": "UTC",
                    "max_active_tasks": 32,
                    "dag_concurrency": 32,
                }
            },
            "unique_views": ["dummy"],
            "unique_entities": ["dummy"],
        },
        deserialize_json=True,
    )

    return overall_config


def get_cutoff_or_warning_time(times: str, timezone: str = "UTC") -> str:
    from datetime import datetime
    from crontab import CronSlices
    from croniter import croniter
    import pytz

    if not times:
        return "00:00"
    cron_patterns = times.split(";")
    timezone = pytz.timezone(timezone)
    now = datetime.now(timezone)

    for pattern in cron_patterns:
        if CronSlices.is_valid(pattern):
            cron_schedule = croniter(pattern, now)
            next_run = cron_schedule.get_next(datetime)

            if next_run.date() == now.date():
                hours = next_run.hour
                minutes = next_run.minute
                return f"{str(hours).zfill(2)}:{str(minutes).zfill(2)}"

        elif ":" in pattern:
            return pattern

    return "00:00"


def get_schedule():
    from datetime import datetime
    from croniter import croniter
    import pytz
    from airflow.models import Variable

    overall_configs = get_overall_config()
    sourcing_config = overall_configs.get("sourcing_config", {})
    timezone = pytz.timezone(
        sourcing_config.get("dag_configs", {}).get("dag_timezone", "UTC")
    )
    if Variable.get("dag_generic_schedule_time", default_var="None") == "None":
        return None
    crons = Variable.get("dag_generic_schedule_time").split(";")
    # Tested with
    # crons = ["0 0 * * MON-WED", "0 0 * * THU", "0 0 * * FRI", "0 0 * * SAT", "0 0 * * SUN"]
    now = datetime.now(timezone)
    nearest_cron = None
    nearest_time = None
    for cron in crons:
        cron_iter = croniter(
            cron, now
        )  # Create a croniter object for each CRON expression
        next_run_time = cron_iter.get_next(datetime)  # Compute the next run time
        # If this is the first CRON expression or the next run time is earlier than the current nearest
        print(next_run_time, nearest_time)
        if nearest_time is None or next_run_time < nearest_time:
            nearest_time = next_run_time
            nearest_cron = cron
    dag_configs = sourcing_config.get("dag_configs")
    dag_configs["next_cron"] = nearest_cron
    sourcing_config["dag_configs"] = dag_configs
    overall_configs["sourcing_config"] = sourcing_config
    Variable.set("next_cron", nearest_cron)
    Variable.set("overall_config", overall_configs, serialize_json=True)
    return nearest_cron
