import re
from airflow.models import Variable
from queries import bigquery_queries as bq_q, snowflake_queries as sf_q
from database_utility.GenericDatabaseConnector import WarehouseConnector
from database_utility.utils import escape_column_name
from database_utility.utils import DATA_TYPE_MAPPINGS


def get_warehouse():
    warehouse = Variable.get("warehouse")
    return warehouse


def get_warehouse_kwargs():
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    return warehouse_kwargs


def build_update_param_col_details_query(entity: str):
    warehouse = get_warehouse()
    warehouse_kwargs = get_warehouse_kwargs()

    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    warehouse_kwargs = eval(warehouse_kwargs)

    if warehouse == "SNOWFLAKE":
        query = sf_q.UPDATE_PARAM_COL_DETAILS.format(
            entity=entity, schema=warehouse_kwargs["schema"]
        )

    elif warehouse == "BIGQUERY":
        query = bq_q.UPDATE_PARAM_COL_DETAILS.format(
            entity=entity,
            project=warehouse_kwargs["project"],
            dataset=warehouse_kwargs["dataset"],
        )
    return query


def build_fetch_params_query(from_clause):
    warehouse = get_warehouse()
    warehouse_kwargs = get_warehouse_kwargs()
    warehouse_kwargs = eval(warehouse_kwargs)
    if warehouse == "BIGQUERY":
        query = bq_q.FETCH_PARAMS.format(
            from_clause=from_clause,
            project=warehouse_kwargs["project"],
            gcp_dataset=warehouse_kwargs["dataset"],
            region=warehouse_kwargs["bigquery_region"],
        )

    if warehouse == "SNOWFLAKE":
        query = sf_q.FETCH_PARAMS.format(
            from_clause=from_clause, schema=warehouse_kwargs["schema"]
        )

    return query


def build_update_filter_param_query(entity, df, is_delete_flag, view_mapping_table):
    import logging

    log = logging.getLogger(__name__)

    warehouse = get_warehouse()
    warehouse_kwargs = get_warehouse_kwargs()

    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    warehouse_kwargs = eval(warehouse_kwargs)

    if len(df) > 0:

        views_df = wc.execute_query(
            f"""Select view from {view_mapping_table} where {escape_column_name('table',warehouse)} = '{entity}' {is_delete_flag}""",
            True,
        )
        views_list = views_df["view"].to_list()

        update_query = ""

        if warehouse == "BIGQUERY":
            for raw_view in views_list:
                normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', raw_view)
                update_query_tmp = f"""
                        DECLARE max_sync_start_datetime_{entity}_{normalized_view_name} DATETIME;
                        DECLARE max_filter_param_{entity}_{normalized_view_name} DATETIME;"""

                update_query += update_query_tmp

        for raw_view in views_list:

            audit_column_name = str(
            Variable.get(f"{raw_view}_param_filter", default_var="SYNCSTARTDATETIME")
        ).upper()
            
            raw_view_full_name = wc._get_complete_table_name(
                raw_view, True
            )

            normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', raw_view)

            update_query_tmp = f"""    
            SET max_filter_param_{entity}_{normalized_view_name} = (SELECT MAX(filter_param) FROM {view_mapping_table} WHERE view='{raw_view}' and {escape_column_name('table',warehouse)} = '{entity}' {is_delete_flag});
            
            SET max_sync_start_datetime_{entity}_{normalized_view_name} = (SELECT CAST(MAX({audit_column_name}) as {DATA_TYPE_MAPPINGS['datetime'][warehouse]}) as {audit_column_name}
            FROM {raw_view_full_name.replace(raw_view, normalized_view_name)}
            WHERE cast({audit_column_name} as {DATA_TYPE_MAPPINGS['datetime'][warehouse]}) >= max_filter_param_{entity}_{normalized_view_name});
            
            UPDATE {view_mapping_table}
            SET filter_param = max_sync_start_datetime_{entity}_{normalized_view_name}
            WHERE view='{raw_view}' and {escape_column_name('table',warehouse)} = '{entity}' {is_delete_flag} ;
            """

            update_query += update_query_tmp

    else:
        if warehouse == "SNOWFLAKE":
            update_query = f"""
            UPDATE {view_mapping_table} 
            SET filter_param=CURRENT_TIMESTAMP()
            WHERE {escape_column_name('table',warehouse)}='{entity}' {is_delete_flag}
            """

        if warehouse == "BIGQUERY":
            update_query = f"""
            UPDATE {view_mapping_table} 
            SET filter_param=CURRENT_DATETIME()
            WHERE {escape_column_name('table',warehouse)}='{entity}' {is_delete_flag}
            """

    log.info("******Update Query******")
    log.info(update_query)
    return update_query


def get_col_for_update_extraction_sync_date_query(view):
    warehouse = get_warehouse()
    warehouse_kwargs = get_warehouse_kwargs()

    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    warehouse_kwargs = eval(warehouse_kwargs)

    audit_column_name = str(
        Variable.get(f"{view}_param_filter", default_var="SYNCSTARTDATETIME")
    ).upper()

    normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', view)

    if warehouse == "BIGQUERY":
        query = f"""
        SELECT 
            column_name 
        FROM {warehouse_kwargs["project"]}.{warehouse_kwargs["dataset"]}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name='{normalized_view_name}'
        AND UPPER(column_name)='{audit_column_name}'"""

    if warehouse == "SNOWFLAKE":
        query = f"""
        SELECT 
            column_name 
        FROM {warehouse_kwargs["schema"]}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name='{normalized_view_name}'
        AND UPPER(column_name)='{audit_column_name}'"""

    return query


def build_update_query_for_update_extraction_sync_date(
    view_mapping_table, view, view_table, is_delete_flag, df
):
    warehouse = get_warehouse()
    warehouse_kwargs = get_warehouse_kwargs()

    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    warehouse_kwargs = eval(warehouse_kwargs)

    audit_column_name = str(
        Variable.get(f"{view}_param_filter", default_var="SYNCSTARTDATETIME")
    ).upper()

    normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', view)

    if len(df) > 0:
        query_job = ""

        if warehouse == "BIGQUERY":
            query_job = f"""
            DECLARE max_sync_start_datetime DATETIME;
            DECLARE max_extraction_sync_dt DATETIME;"""

        query_job = (
            query_job
            + f"""SET max_extraction_sync_dt = (SELECT MAX(extraction_sync_dt) FROM {view_mapping_table} WHERE view='{view}' {is_delete_flag});

        SET max_sync_start_datetime = (SELECT CAST(MAX({audit_column_name}) as {DATA_TYPE_MAPPINGS['datetime'][warehouse]}) as {audit_column_name}
        FROM {view_table.replace(view, normalized_view_name)}
        WHERE cast({audit_column_name} as {DATA_TYPE_MAPPINGS['datetime'][warehouse]}) >= max_extraction_sync_dt);


        UPDATE {view_mapping_table}
        SET extraction_sync_dt = max_sync_start_datetime
        WHERE view='{view}'  {is_delete_flag} ;

        """
        )

    else:
        if warehouse == "BIGQUERY":
            query_job = f""" UPDATE {view_mapping_table} 
                SET extraction_sync_dt=CURRENT_DATETIME()
                WHERE view='{view}' {is_delete_flag}
                """
        elif warehouse == "SNOWFLAKE":
            query_job = f""" UPDATE {view_mapping_table} 
            SET extraction_sync_dt=CURRENT_TIMESTAMP
            WHERE view='{view}' {is_delete_flag}
            """

    return query_job


def build_fetch_latest_date_query(view_name, param_filter):
    warehouse = get_warehouse()
    if warehouse == "BIGQUERY":
        query = bq_q.FETCH_LATEST_DATE.format(view_name=view_name, param_filter=param_filter)
    elif warehouse == "SNOWFLAKE":
        query = sf_q.FETCH_LATEST_DATE.format(view_name=view_name, param_filter=param_filter)
    else:
        raise ValueError("Unsupported warehouse. Supported values are: 'BIGQUERY', 'SNOWFLAKE'.")
    return query