from airflow.models import Variable
from queries import bigquery_queries as bq_q, snowflake_queries as sf_q
from database_utility.GenericDatabaseConnector import WarehouseConnector
from datetime import datetime


def build_transform_validate_query(
    validated_table_name: str,
    raw_table_name: str,
    anomaly_table_name: str,
    anomaly_summary_table_name: str,
    except_cols_const: str,
    transformed_cols_final,
    where_filter_clause: str,
    old_cols: str,
    typecast_cols_const: str,
    null_cols_const: str,
    level_cols_const: str,
    entity_type: str,
    null_check_const: str,
):
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    if warehouse == "SNOWFLAKE":
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        sub_query = f"""
        SELECT
            LISTAGG(c.COLUMN_NAME, ', ') WITHIN GROUP(ORDER BY c.COLUMN_NAME) AS column_list,
            ANY_VALUE(c.TABLE_SCHEMA || '.' || c.TABLE_NAME) AS full_table_name,
            LISTAGG(REPLACE(SPACE(6) || ',total_rows - COUNT(' || c.COLUMN_NAME || ') AS ' || c.COLUMN_NAME 
                                    || CHAR(13), '<col_name>', c.COLUMN_NAME), '') 
            WITHIN GROUP(ORDER BY COLUMN_NAME) AS column_count_list,
            REPLACE(REPLACE(REPLACE(
        'WITH cte AS (
        SELECT
            COUNT(*) AS total_rows
        <column_count_list>
        FROM <table_name>
        )
        SELECT COLUMN_NAME, NULLS_COLUMN_COUNT as nulls_count
        FROM cte
        UNPIVOT (NULLS_COLUMN_COUNT FOR COLUMN_NAME IN (<column_list>))
        ORDER BY COLUMN_NAME;'
            ,'<column_count_list>', column_count_list)
            ,'<table_name>', full_table_name)
            ,'<column_list>', column_list) AS query_to_run
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE TABLE_SCHEMA = UPPER('{eval(warehouse_kwargs)['schema']}')
        AND TABLE_NAME = UPPER('{entity_type}_validated_table')
        AND c.COLUMN_NAME in  ('{null_check_const}') 
        """
        df = wc.execute_query(sub_query, True)
        null_query = (
            df["query_to_run"].iloc[0]
            if df["query_to_run"].iloc[0]
            else "select null as col_name, null as nulls_count"
        )

        _query = sf_q.TRANSFORM_VALIDATE.format(
            validated_table_name=validated_table_name,
            raw_table_name=raw_table_name,
            anomaly_table_name=anomaly_table_name,
            anomaly_summary_table_name=anomaly_summary_table_name,
            except_cols_const=except_cols_const,
            transformed_cols_final=transformed_cols_final,
            where_filter_clause=where_filter_clause,
            old_cols=old_cols,
            typecast_cols_const=typecast_cols_const,
            null_cols_const=null_cols_const,
            level_cols_const=level_cols_const,
            entity_type=entity_type,
            null_check_const=null_check_const,
            null_query=null_query,
        )

    elif warehouse == "BIGQUERY":
        _query = bq_q.TRANSFORM_VALIDATE.format(
            validated_table_name=validated_table_name,
            raw_table_name=raw_table_name,
            anomaly_table_name=anomaly_table_name,
            anomaly_summary_table_name=anomaly_summary_table_name,
            except_cols_const=except_cols_const,
            transformed_cols_final=transformed_cols_final,
            where_filter_clause=where_filter_clause,
            old_cols=old_cols,
            typecast_cols_const=typecast_cols_const,
            null_cols_const=null_cols_const,
            level_cols_const=level_cols_const,
            entity_type=entity_type,
            null_check_const=null_check_const,
        )

    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    return _query


def build_anomaly_metric_summary_query(
    validated_table_name: str,
    raw_table_name: str,
    null_cols_const: str,
    level_cols_const: str,
    where_filter_clause: str,
):
    warehouse = Variable.get("warehouse")
    if warehouse == "SNOWFLAKE":
        _query = sf_q.ANOMALY_METRIC_QUERY.format(
            validated_table_name=validated_table_name,
            raw_table_name=raw_table_name,
            null_cols_const=null_cols_const,
            level_cols_const=level_cols_const,
            where_filter_clause=where_filter_clause,
        )

    elif warehouse == "BIGQUERY":
        _query = bq_q.ANOMALY_METRIC_QUERY.format(
            validated_table_name=validated_table_name,
            raw_table_name=raw_table_name,
            null_cols_const=null_cols_const,
            level_cols_const=level_cols_const,
            where_filter_clause=where_filter_clause,
        )

    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    return _query


def merge_query_base_masters(
    destination_table_name,
    source_table_name,
    level_const,
    master_table_query,
    on_condition_const,
    optimization_partition_clause,
    delete_query,
    master_cols_const,
    source_master_cols_const,
    update_set_const,
    pull_type,
    class_type,
    entity_type,
    attribute_hierarchy_list=None,
):
    warehouse = Variable.get("warehouse")
    if warehouse == "SNOWFLAKE":
        _query = sf_q.MERGE_QUERY.format(
            destination_table=destination_table_name,
            master_table_query=master_table_query,
            on_condition_const=on_condition_const,
            master_cols_const=master_cols_const,
            source_master_cols_const=source_master_cols_const,
            update_set_const=update_set_const,
        )
        _step_query = ""
        if class_type == "B":
            if not (entity_type in attribute_hierarchy_list):
                if pull_type == "full":
                    _step_query = sf_q.DELETE_MERGE_QUERY.format(
                        destination_table=destination_table_name,
                        level_const=level_const,
                        source_table=source_table_name,
                    )
            else:
                _step_query = sf_q.UPDATE_MERGE_QUERY.format(
                    destination_table=destination_table_name,
                    level_const=level_const,
                    source_table=source_table_name,
                )

        elif class_type == "A":
            if pull_type == "full":
                _step_query = sf_q.DELETE_MERGE_QUERY.format(
                    destination_table=destination_table_name,
                    level_const=level_const,
                    source_table=source_table_name,
                )
        _query = _query + _step_query

    elif warehouse == "BIGQUERY":
        _query = bq_q.MERGE_QUERY.format(
            destination_table=destination_table_name,
            master_table_query=master_table_query,
            on_condition_const=on_condition_const,
            optimization_clause=optimization_partition_clause,
            delete_query=delete_query,
            master_cols_const=master_cols_const,
            source_master_cols_const=source_master_cols_const,
            update_set_const=update_set_const,
        )

    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    return _query


def build_export_anomaly_data_to_gcs_query(entity: str):
    warehouse = Variable.get("warehouse", default_var="BIGQUERY")
    warehouse_kwargs = Variable.get("warehouse_kwargs", default_var={})

    # f'gs://{{{{ var.value.get("tenant_alias") }}}}-{{{{ var.value.get("pipeline") }}}}-anomaly-data/{str(datetime.now())[:10]}/{{{{ et }}}}/*.{{{{ var.value.get("anomaly_data_file_type", "csv") }}}}'
    anomaly_data_file_delimiter = Variable.get("anomaly_data_file_delimiter", "|")
    if warehouse == "SNOWFLAKE":
        gcs_uri = f'gcs://{Variable.get("tenant_alias")}-{Variable.get("pipeline")}-anomaly-data/{str(datetime.now())[:10]}/{entity}/*.{Variable.get("anomaly_data_file_type", "csv")}'
        database = eval(warehouse_kwargs)["database"]
        schema = eval(warehouse_kwargs)["schema"]
        storage_integration = eval(warehouse_kwargs)["storage_integration"]

        _query = sf_q.COPY_ANOMALY_DATA_TO_SNOWFLAKE.format(
            gcs_uri=gcs_uri,
            et=entity,
            database=database,
            schema=schema,
            storage_integration=storage_integration,
            anomaly_data_file_delimiter=anomaly_data_file_delimiter,
        )

    elif warehouse == "BIGQUERY":
        gcs_uri = f'gs://{Variable.get("tenant_alias")}-{Variable.get("pipeline")}-anomaly-data/{str(datetime.now())[:10]}/{entity}/*.{Variable.get("anomaly_data_file_type", "csv")}'
        gcp_project = Variable.get(
            "gcp_project"
        )  
        dataset = Variable.get("dataset")

        _query = bq_q.COPY_ANOMALY_DATA_TO_GCS.format(
            et=entity,
            gcs_uri=gcs_uri,
            gcp_project=gcp_project,
            dataset=dataset,
            anomaly_data_file_delimiter=anomaly_data_file_delimiter,
        )

    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    Variable.set(f"{entity}_anomaly_to_gcs_query", _query)
    return _query


def build_master_table_transaction(master_table, master_validated_table, master_cols_const, backfill):
    warehouse = Variable.get("warehouse")
    if warehouse == "SNOWFLAKE":
        _query = sf_q.MASTER_TABLE_TRANSCATION.format(master_table=master_table, master_validated_table=master_validated_table, 
                                                      master_cols_const=master_cols_const, backfill=backfill)
    elif warehouse == "BIGQUERY":
        _query = bq_q.MASTER_TABLE_TRANSCATION.format(master_table = master_table, master_validated_table = master_validated_table, 
                                                        master_cols_const = master_cols_const, backfill = backfill)
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    return _query

def build_lsi_mv(master_table_imputed, master_table):
    warehouse = Variable.get("warehouse")
    if warehouse == "SNOWFLAKE":
        _query = sf_q.LSI_QUERY_MV.format(master_table_imputed=master_table_imputed, master_table=master_table)
    elif warehouse == "BIGQUERY":
        _query = bq_q.LSI_QUERY_MV.format(master_table_imputed=master_table_imputed, master_table=master_table)
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    return _query

def build_slots_query(jobs, start_time, region):
    warehouse = Variable.get("warehouse")
    if warehouse == "SNOWFLAKE":
        _query = sf_q.SLOTS_QUERY.format(jobs=jobs, start_time=start_time)
    elif warehouse == "BIGQUERY":
        _query = bq_q.SLOTS_QUERY.format(jobs=jobs, start_time=start_time, region=region)
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    return _query

def build_mp_bucket_code_historic_query(table_name, hierarchies, num_products_per_bucket, window_partition, join_condition):
    warehouse = Variable.get("warehouse")
    if warehouse == "SNOWFLAKE":
        hierarchy_split_concat = 'CONCAT(' + ','.join([f'SUBSTRING(SHA2({h}, 256),1,5)' for h in hierarchies.split(",")]) +') AS psuedo_hierarchy_level'
        _query = sf_q.MP_BUCKET_HISTORIC_QUERY.format(table_name = table_name, hierarchies = hierarchies,
                                          num_products_per_bucket = num_products_per_bucket, window_partition = window_partition, 
                                          join_condition = join_condition, hierarchy_split_concat =hierarchy_split_concat)
    elif warehouse == "BIGQUERY":
        hierarchy_split_concat = 'CONCAT(' + ','.join([f'SUBSTR(TO_HEX(SHA256({h})),0,5)' for h in hierarchies.split(",")]) +') AS psuedo_hierarchy_level'
        _query = bq_q.MP_BUCKET_HISTORIC_QUERY.format(table_name = table_name, hierarchies = hierarchies,
                                          num_products_per_bucket = num_products_per_bucket, window_partition = window_partition, 
                                          join_condition = join_condition, hierarchy_split_concat =hierarchy_split_concat)
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    return _query


def build_mp_bucket_code_periodic_query(table_name, bckp_table_name, hierarchies, num_products_per_bucket, window_partition, join_condition):
    warehouse = Variable.get("warehouse")
    hierarchy_split = ','.join(['T1.'+hierarchy for hierarchy in hierarchies.split(',')])
    if warehouse == "SNOWFLAKE":
        hierarchy_split_concat = 'CONCAT(' + ','.join([f'SUBSTRING(SHA2(T1.{h}, 256),1,5)' for h in hierarchies.split(",")]) +') AS psuedo_hierarchy_level'
                                
        _query = sf_q.MP_BUCKET_PERIODIC_QUERY.format(table_name = table_name, bckp_table_name = bckp_table_name, hierarchies = hierarchies,
                                          num_products_per_bucket = num_products_per_bucket, window_partition = window_partition, 
                                          join_condition = join_condition, hierarchy_split_concat =hierarchy_split_concat, hierarchy_split=hierarchy_split)
    elif warehouse == "BIGQUERY":
        hierarchy_split_concat = 'CONCAT(' + ','.join([f'SUBSTR(TO_HEX(SHA256(T1.{h})),0,5)' for h in hierarchies.split(",")]) +') AS psuedo_hierarchy_level'
                                    
        _query = bq_q.MP_BUCKET_PERIODIC_QUERY.format(table_name = table_name, bckp_table_name = bckp_table_name, hierarchies = hierarchies,
                                          num_products_per_bucket = num_products_per_bucket, window_partition = window_partition, 
                                          join_condition = join_condition, hierarchy_split_concat = hierarchy_split_concat, hierarchy_split=hierarchy_split)
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse}")

    return _query