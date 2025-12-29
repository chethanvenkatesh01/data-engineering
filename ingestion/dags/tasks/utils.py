from airflow.utils.log.logging_mixin import LoggingMixin
from database_utility.GenericDatabaseConnector import WarehouseConnector

log = LoggingMixin().log

def get_db_engine():
    from airflow.models import Variable
    import urllib
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    conn_string = (
        "postgresql://"
        + Variable.get("PGXUSER")
        + ":"
        + urllib.parse.quote(Variable.get("PGXPASSWORD"))
        + "@"
        + Variable.get("PGXHOST")
        + ":"
        + Variable.get("PGXPORT")
        + "/"
        + Variable.get("PGXDATABASE")
    )
    db_engine = create_engine(conn_string, poolclass=NullPool)
    return db_engine


def create_dynamic_constants(file_name, content, json_file=False):
    import os
    import json
    rel_file_path = f"constants/dynamic_constants/{file_name}" 
    file_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', rel_file_path))
    with open(file_path, "w") as f:
        if json_file:
            json.dump(content, f, indent=4, default=str)
        else:
            f.write(content)
    
    return True

def read_json_file(file_name):
    import os
    import json
    rel_file_path = f"constants/dynamic_constants/{file_name}" 
    file_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', rel_file_path))
    with open(file_path) as f:
        data = json.load(f)
    
    return data
        


def idtoken_from_metadata_server(url: str, service_account_email: str = ''):

    from google.auth.transport.requests import Request
    from google.auth import compute_engine
    request = Request()
    # the service_account_email is used if passed through the request
    if service_account_email:
        credentials = compute_engine.IDTokenCredentials(
            request=request,
            target_audience=url,
            use_metadata_identity_endpoint=False,
            service_account_email=service_account_email,
        )
    else:
        # else the default service account attached to the compute engine is used
        credentials = compute_engine.IDTokenCredentials(
            request=request, target_audience=url, use_metadata_identity_endpoint=True
        )

    credentials.refresh(request)
    return credentials.token


def psql_to_gbq(psq_table: str, bq_table: str) -> None:
    
    import pandas as pd
    from airflow.models import Variable

    """
    Function used to copy table from postgresql to google bigQuery

    Args:
        psq_table (str) : table name in postgress
        bq_table (str) : destination table name in bigquery
    """
    db_engine = get_db_engine()
    db_connection = db_engine.connect()
    temp_dataframe = pd.read_sql(
        f"""SELECT * FROM {Variable.get("PGXSCHEMA")}.{psq_table}""",
        db_connection,
    )

    # transfer of table from dataframe memory to bigquery
    temp_dataframe.to_gbq(
        f"""{Variable.get("dataset")}.{bq_table}""",
        project_id=Variable.get("gcp_project"),
        if_exists="replace",
    )


def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    from io import StringIO
    import csv
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def get_impute_features():
    
    import pandas as pd
    from airflow.models import Variable

    impute_query = f"""
            select concat('{{ ',string_agg(temp ,' ,'), ' }}') as impute_features
            from 
            (select concat( '"',feature_type,'"', ' : ', '[ "' ,STRING_AGG(column_name, '" , "'),'"]') as temp
            from {Variable.get("PGXSCHEMA")}.feature_metadata
            where entity in ('impute_features')
            group by feature_type ) as a
    """
    db_engine = get_db_engine()
    impute_features_data = pd.read_sql(impute_query, db_engine)
    impute_features = eval(impute_features_data.impute_features.iloc[0])

    if "discount" not in impute_features.keys():
        impute_features["discount"] = []

    Variable.set("impute_features", impute_features)

    if eval(Variable.get("run_lsi_flag")) == True:
        lost_sales_query = f"""select concat('{{ ',string_agg(temp ,' ,'), ' }}') as lost_sales
                from 
                (select concat( '"',column_name,'"', ' : ' ,STRING_AGG(gbq_formula, '" , "')) as temp
                from {Variable.get("PGXSCHEMA")}.feature_metadata
                where entity in ('lost_sales')
                group by column_name ) as a """

        lost_sales_data = pd.read_sql(lost_sales_query, db_engine)
        lost_sales = eval(lost_sales_data.lost_sales.iloc[0])

        Variable.set("active_stores_cutoff", lost_sales["active_stores_cutoff"])
        Variable.set("sell_through_cutoff", lost_sales["sell_through_cutoff"])
        Variable.set("lsi_channel", lost_sales["lsi_channel"])

        price_impute_string = f"""
                    select string_agg(temp ,' , ') as price_impute_string
                            from 
                            (select concat( 'coalesce( a.',column_name,'_imputed', ' , ' , 'b.', gbq_formula,') as ', column_name, '_imputed') as temp
                            from {Variable.get("PGXSCHEMA")}.feature_metadata
                            where entity IN ('impute_features') AND feature_type IN ('price') and gbq_formula is not null
                            group by 1 ) as a
                    """

        price_impute_data = pd.read_sql(price_impute_string, db_engine)
        Variable.set(
            "price_impute_string", price_impute_data.price_impute_string.iloc[0]
        )


def generic_mapping_data():

    import pandas as pd
    from airflow.models import Variable
    from google.cloud import bigquery
    from constants.constant import get_overall_config

    db_engine = get_db_engine()
    generic_master_mapping = pd.read_sql_table(
        "generic_master_mapping", 
        db_engine, 
        schema=Variable.get("PGXSCHEMA")
    )

    generic_master_mapping_unfiltered = generic_master_mapping.copy()
    generic_master_mapping = generic_master_mapping[
        generic_master_mapping["to_be_ingested"] == True
    ]
    product_mapping = pd.read_sql_table(
        "product_generic_schema_mapping",
        db_engine,
        schema=Variable.get("PGXSCHEMA"),
    )

    all_hierarchies = (
        product_mapping[product_mapping["is_hierarchy"] == True]
        .sort_values("hierarchy_level")
        .generic_column_name.to_list()
    )
    all_hierarchies_reverse = (
        product_mapping[product_mapping["is_hierarchy"] == True]
        .sort_values("hierarchy_level", ascending=False)
        .generic_column_name.to_list()
    )
    h_levels = (
        product_mapping[product_mapping["is_hierarchy"] == True]
        .sort_values("hierarchy_level")
        .hierarchy_level.to_list()
    )
    all_hierarchies_levels = dict(zip(all_hierarchies, h_levels))
    Variable.set("all_hierarchies", all_hierarchies)
    Variable.set("all_hierarchies_reverse", all_hierarchies_reverse)
    Variable.set("all_hierarchies_levels", all_hierarchies_levels)

    pull_type = generic_master_mapping.pull_type.to_list()
    raw_table = generic_master_mapping.source_table.to_list()
    destination_table = generic_master_mapping.destination_table.to_list()
    pg_persistence = generic_master_mapping.persist_to_postgres.to_list()
    mapping_table = generic_master_mapping.generic_mapping_table.to_list()
    attribute_table_list_temp = generic_master_mapping[
        generic_master_mapping["persist_attributes_table"] == True
    ].destination_table.to_list()
    hierarchy_table_list_temp = generic_master_mapping[
        generic_master_mapping["persist_hierarchies_table"] == True
    ].destination_table.to_list()
    data_loss_threshold = generic_master_mapping.data_loss_threshold.to_list()

    attribute_table_list = []
    hierarchy_table_list = []
    for i, j in zip(attribute_table_list_temp, hierarchy_table_list_temp):
        attribute_table_list.append(i.split("_")[0])
        hierarchy_table_list.append(j.split("_")[0])

    attribute_hierarchy_list = list(set(attribute_table_list + hierarchy_table_list))
    Variable.set("pull_type", pull_type)
    Variable.set("destination_table", destination_table)
    Variable.set("raw_table", raw_table)
    Variable.set("mapping_table", mapping_table)
    Variable.set("attribute_table_list", attribute_table_list)
    Variable.set("hierarchy_table_list", hierarchy_table_list)
    Variable.set("attribute_hierarchy_list", attribute_hierarchy_list)
    Variable.set("pg_persistence", pg_persistence)
    
    # data loss needs to be configured in generic master mapping.
    Variable.set("data_loss_threshold", data_loss_threshold)
    
    wc = WarehouseConnector(Variable.get("warehouse"), Variable.get("warehouse_kwargs"))
    rm_table_name = wc._get_complete_table_name(Variable.get('rule_master_name'), False)
    #set qc entities and tables
    query_string = f"""
        SELECT *
        FROM {rm_table_name}
    """
    if Variable.get('read_from_db_flag'):
        rule_engine = pd.read_sql_table(
            "rule_master", 
            db_engine, 
            schema='data_platform'
        )
    else:
        rule_engine=wc.execute_query(query_string, True)
        

    qc_entity_tabs = [list(eval(rule_engine['table'][i]).keys())[0] for i in range(len(rule_engine))]

    generic_schema_mapping_list = list(
        generic_master_mapping_unfiltered["source_table"]
    )
    qc_entity_tabs.extend(generic_schema_mapping_list)

    qc_query="""
        SELECT * 
        FROM (
            select table_name                  
            from 
                data_platform.custom_qc qc
            left join 
                data_platform.table_info ti
            on qc.table_id=ti.table_id
            ) as a 
    """

    qc_data = pd.read_sql(qc_query, db_engine)
    custom_qc_table_list = list(qc_data.table_name)
    qc_entity_tabs.extend(custom_qc_table_list)
    Variable.set("qc_entities_and_table_list", list(set(qc_entity_tabs)))

    entity_type = []
    for i in eval(Variable.get("destination_table","['dummy]")):
        entity_type.append(i.rsplit("_", 1)[0])
    Variable.set("entity_type", entity_type)

    entities_configuration = {
        "destination_table":destination_table,
        "pull_type":pull_type,
        "data_loss_threshold":data_loss_threshold,
        "pg_persistence":pg_persistence,
        "raw_table":raw_table,
        "mapping_table":mapping_table,
        "attribute_table_list":attribute_table_list,
        "attribute_hierarchy_list":attribute_hierarchy_list,
        "entity_type":entity_type
    }
    overall_config = get_overall_config()
    overall_config["entities_config"] = entities_configuration
    Variable.set("overall_config", overall_config, serialize_json=True)


def read_mapping_data(raw_table, mapping_table, entity_type, pull_type, data_loss_threshold):
    
    import pandas as pd
    from airflow.models import Variable

    wc = WarehouseConnector(Variable.get("warehouse"), Variable.get("warehouse_kwargs"))
    raw_table_complete_name = wc._get_complete_table_name(raw_table, True)
    raw_table_query = f""" select * from {raw_table_complete_name} limit 0 """
    db_engine = get_db_engine()
    mapping_data = pd.read_sql_table(mapping_table, db_engine, schema=Variable.get("PGXSCHEMA"))
    level = mapping_data[mapping_data["unique_by"] == True].generic_column_name.to_list()
    raw_data = wc.execute_query(raw_table_query, True)
    mapping_data = mapping_data[mapping_data.generic_column_name.notnull()].to_dict()
    raw_data = raw_data.to_dict()
    Variable.set(f"level_{entity_type}", level)
    Variable.set(f"mapping_data_{entity_type}", mapping_data)
    Variable.set(f"raw_data_{entity_type}", raw_data)
    where_filter = ""
    Variable.set(f"pull_type_{entity_type}", pull_type)
    Variable.set(f"data_loss_threshold_{entity_type}", data_loss_threshold)
    if pull_type == "incremental":
        # for old clients
        if Variable.get("view_mapping_table") == Variable.get("sourcing_mapping_table_name"):
            mapping_table_complete_name= wc._get_complete_table_name(Variable.get('sourcing_mapping_table_name'), True)
            read_sourcing_mapping_table = f"""
                select dataingestion_filterparam, filter
                from {mapping_table_complete_name}
                where intermediate_table = '{raw_table}'
                limit 1
            """
        # for new clients
        else:
            mapping_table_complete_name = wc._get_complete_table_name(Variable.get('sourcing_mapping_table_name'), True)
            view_mapping_complete_name = wc._get_complete_table_name(Variable.get('view_mapping_table'), True)
            read_sourcing_mapping_table = f"""
                SELECT v.filter, i.dataingestion_filterparam FROM {view_mapping_complete_name} as v 
                JOIN {mapping_table_complete_name} as i 
                ON v.table=i.table WHERE v.is_deleted=False and i.is_deleted = False and v.view IS NOT NULL and i.intermediate_table = '{raw_table}'
                group by 1,2
                limit 1
            """
        # read_sourcing_mapping_table = pd.read_gbq(
        #     read_sourcing_mapping_table,
        #     project_id=Variable.get("gcp_project"),
        #     location=Variable.get("bigquery_region", default_var="US")
        # )
        read_sourcing_mapping_table = wc.execute_query(read_sourcing_mapping_table, True)
        where_filter = f"where {read_sourcing_mapping_table['filter'].to_list()[0]} > '{read_sourcing_mapping_table['dataingestion_filterparam'].to_list()[0]}'"
        Variable.set(
            f"periodic_filter_name_{entity_type}",
            read_sourcing_mapping_table["filter"].to_list()[0],
        )

    Variable.set(f"where_filter_{entity_type}", where_filter)


def get_unique_partition_values(entity_type):
    
    import pandas as pd
    from airflow.models import Variable
    from google.cloud import bigquery

    mapping_data = pd.DataFrame.from_dict(
        eval(
            Variable.get(f"mapping_data_{entity_type}", default_var="0").replace(
                "nan", "None"
            )
        )
    )

    partition_columns = mapping_data[mapping_data["is_partition_col"] == True]

    assert len(partition_columns) <= 1, "partition_col has more than 1 element."
    if len(partition_columns) > 0:
        partition_col_name = str(partition_columns.iloc[0].generic_column_name)
        query_string = f"""
        select concat("( '",string_agg(cast({partition_col_name} as string), "', '"),"' )") as distinct_values
        from (select distinct {partition_col_name}
        FROM {Variable.get('gcp_project')}.{Variable.get('dataset')}.{entity_type}_validated_table)
        """

        bqclient = bigquery.Client(
            project=Variable.get("gcp_project"),
            location=Variable.get("bigquery_region", default_var="US")
        )
        df = bqclient.query(query_string).to_dataframe()
        if df.iloc[0, 0]:
            in_clause_value = df["distinct_values"].iloc[0]
            complete_clause = f" and target.{partition_col_name} in {in_clause_value}"
            Variable.set(
                f"optimization_partition_clause_{entity_type}", complete_clause
            )
        else:
            Variable.set(
                f"optimization_partition_clause_{entity_type}", " and false"
            )
    else:
        Variable.set(f"optimization_partition_clause_{entity_type}", " ")


def set_anomaly_tables_list():
    import pandas as pd
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    """
    Method to fetch all the *_anomaly tables.

    @returns: None
    """
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    data=wc._get_table_list('%_anomaly')
    data_set = set(data)
    Variable.set("anomaly_tables", data_set)


def data_anomaly_check(entity_type):

    import pandas as pd
    from airflow.models import Variable
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    anomaly_table_name = wc._get_complete_table_name(f"{entity_type}_anomaly_summary", False)
    validated_table_name = wc._get_complete_table_name(f"{entity_type}_validated_table", False)

    query = f"""
        with a as(
        select  max(no_original_rows) as no_original_rows 
                from {anomaly_table_name}),
        b as(
        select count(*) as final_count from {validated_table_name})

        select CASE 
                    WHEN a.no_original_rows = 0 THEN NULL
                    ELSE ((a.no_original_rows - b.final_count) * 100.0) / a.no_original_rows
                END as total_loss from a, b
        """

    data = wc.execute_query(query, True)
    if data.empty or "total_loss" not in data.columns or data.total_loss.isna().iloc[0]:
        total_loss = 0
    else:
        total_loss = data.total_loss.iloc[0]
    print('total_loss:',total_loss)
    if float(total_loss) > float(
        Variable.get(f"data_loss_threshold_{entity_type}")
    ):
        Variable.set(f"validation_status_{entity_type}", False)
    else:
        Variable.set(f"validation_status_{entity_type}", True)
    return True


def copy_validated_table_to_postgres(validated_table):
    
    import pandas as pd
    from airflow.models import Variable
    
    db_engine = get_db_engine()
    validated_table_query = f"select * from {Variable.get('gcp_project')}.{Variable.get('dataset')}.{validated_table}"
    validated_table_bq = pd.read_gbq(
        validated_table_query,
        project_id=Variable.get("gcp_project"),
        location=Variable.get("bigquery_region", default_var="US")
    )
    validated_table_bq.to_sql(
        validated_table.lower(),
        db_engine,
        index=False,
        if_exists="replace",
        method=psql_insert_copy,
        schema=Variable.get("PGXSCHEMA"),
    )


def fetch_latest_sync_date(et, rt):

    import pandas as pd
    from datetime import date
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector
        
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    today = date.today()
    today = str(today).replace("-", "_")
    periodic_filter_name = Variable.get(f"periodic_filter_name_{et}")
    validated_table_name = wc._get_complete_table_name(f'{et}_validated_table', False)
    latest_intermediate_ingestion_date = f"""
        select (max({periodic_filter_name})) as latest_intermediate_ingestion_date
        from {validated_table_name}
    """
    latest_intermediate_ingestion_date = wc.execute_query(latest_intermediate_ingestion_date, True)
    latest_intermediate_ingestion_date = latest_intermediate_ingestion_date[
        "latest_intermediate_ingestion_date"
    ].to_list()[0]
    Variable.set(
        f"latest_intermediate_ingestion_date_{et}", latest_intermediate_ingestion_date
    )


def download_blob_content(bucket_name, source_blob_name):
    from google.cloud import storage
    """Downloads a single blob from GCS."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_string().decode("utf-8")


def update_to_latest_syncdate_udpate(et, rt):
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector
        
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    latest_date = Variable.get(f"latest_intermediate_ingestion_date_{et}")
    if latest_date in ["None", "NaT"]:
        query = "select 'dummy'"
    else:
        sourcing_mapping_table = wc._get_complete_table_name(Variable.get('sourcing_mapping_table_name'), True)
        query = f"""
            update  {sourcing_mapping_table} 
            set dataingestion_filterparam =
            (case when intermediate_table = '{rt}' then cast('{latest_date}' as datetime) end )
            where intermediate_table='{rt}'
        """
    Variable.set(f"update_mapping_table_with_latest_date_query_{et}", query)


def fetch_derived_tables_data():

    import pandas as pd
    from constants.constant import DERIVED_TABLES_MAPPING_TABLE, ENV_SCHEMA, get_overall_config
    from google.cloud import storage
    from airflow.models import Variable
    from croniter import croniter
    from airflow.exceptions import AirflowFailException
    from concurrent import futures
    from datetime import datetime, timedelta
    from numpy import nan
    from database_utility.GenericDatabaseConnector import WarehouseConnector
        
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    try:
        # fetch the derived tables mapping table
        if eval(Variable.get("read_from_db_flag", default_var="False")):
            df = run_db_query(
                f"""
                    select name, run_in, type, replace_flag_gbq, replace_flag_psg, execution_order, schedule_interval
                    from {ENV_SCHEMA}.{DERIVED_TABLES_MAPPING_TABLE}
                    where execution_order > 0 and is_deleted = false
                    order by execution_order
                """
            )
        else:
            derived_table_mapping_name = wc._get_complete_table_name(DERIVED_TABLES_MAPPING_TABLE, False)
            df = wc.execute_query(
                f"""
                    select * 
                    from {derived_table_mapping_name}
                    where execution_order > 0
                    order by execution_order
                """,
               True
            )

        # skip derived table if the mapping table is empty
        if df.empty:
            Variable.set("ERROR__derived_tables", "Mapping table is empty")
            Variable.set("der_tabs_names_lst", [])
            Variable.set("der_tabs_exec_order_dict", {})
            return True

        # fetch the queries to run from json file
        client = storage.Client()
        bucket = client.get_bucket(Variable.get("gcs_bucket"))
        source_folder = Variable.get("derived_tables_queries_loc")
        blobs = list(bucket.list_blobs(prefix=source_folder))

        json_file = {}

        with futures.ThreadPoolExecutor(max_workers=10) as executor:
            # download_blob
            # Create a future to blob content mapping
            future_to_blob = {
                executor.submit(download_blob_content, bucket, blob.name): blob
                for blob in blobs
                if not blob.name.endswith("/")
            }
            # As each thread completes, get the result and add it to the JSON object
            for future in futures.as_completed(future_to_blob):
                blob = future_to_blob[future]
                blob_name = blob.name
                try:
                    content = future.result()
                    file_name_without_extension = blob_name.split("/")[-1].split(".")[0]
                    json_file[file_name_without_extension] = content
                except Exception as exc:
                    log.info(f"Blob {blob.name} generated an exception: {exc}")

        log.info("intermediate tables json_file keys", json_file.keys())
        """
        blob = bucket.get_blob()
        file = json.loads(blob.download_as_text(encoding="utf-8"))
        """

        df = (
            df[df.execution_order > 0]
            .sort_values("execution_order")
            .reset_index(drop=True)
            .set_index("name")
        )
        df['replace_flag_gbq'] = df['replace_flag_gbq'].astype(str)
        df['replace_flag_psg'] = df['replace_flag_psg'].astype(str)
        df['execution_order'] = df['execution_order'].astype(int)

        if 'db_schema' not in df.columns:
            df['db_schema'] = nan
        
        der_tabs_names_lst = df.index.to_list()

        Variable.set("der_tabs_copy_to_psg_job_position", "None")
        der_tabs_copy_to_psg_job_position = None
        remove_because_not_scheduled = []
        remove_because_no_query = []
        der_tabs_info_dict={}
        # set the variables
        first_gbq_job = None
        for tab in der_tabs_names_lst:
            # set the order at which PSG execution starts
            if first_gbq_job is None:
                if df.loc[tab, "run_in"] == "gbq":
                    first_gbq_job = int(df.loc[tab, "execution_order"])
            if (
                Variable.get(
                    "der_tabs_copy_to_psg_job_position", default_var="None"
                )
                == "None"
                and df.loc[tab, "run_in"] == "psg"
            ) and first_gbq_job:
                Variable.set(
                    "der_tabs_copy_to_psg_job_position", int(df.loc[tab, "execution_order"])
                )
                der_tabs_copy_to_psg_job_position=int(df.loc[tab, "execution_order"])

            # set derived tables info 
            der_tabs_info_dict[tab] = {
                "name":tab,
                "run_in":df.loc[tab, "run_in"],
                "type":df.loc[tab, "type"],
                "replace_flag_gbq":df.loc[tab, "replace_flag_gbq"],
                "replace_flag_psg":df.loc[tab, "replace_flag_psg"],
                "execution_order":int(df.loc[tab, "execution_order"]),
                "schedule_interval":df.loc[tab, "schedule_interval"],
                "db_schema": df.loc[tab, "db_schema"]
            }
            # set the variables
            Variable.set(f"der_tab_{tab}_name", tab)
            Variable.set(f"der_tab_{tab}_run_in", df.loc[tab, "run_in"])
            Variable.set(f"der_tab_{tab}_type", df.loc[tab, "type"])
            Variable.set(f"der_tab_{tab}_replace_flag_gbq", df.loc[tab, "replace_flag_gbq"])
            Variable.set(f"der_tab_{tab}_replace_flag_psg", df.loc[tab, "replace_flag_psg"])
            Variable.set(f"der_tab_{tab}_execution_order", int(df.loc[tab, "execution_order"]))
            Variable.set(f"der_tab_{tab}_schedule_interval", df.loc[tab, "schedule_interval"])
            Variable.set(f"der_tab_{tab}_db_schema", df.loc[tab, "db_schema"])

            # if the task is set to copy the table to postgres fail the task
            if (
                (Variable.get(f"der_tab_{tab}_type") == "procedure")
                and (Variable.get(f"der_tab_{tab}_run_in") == "gbq")
                and (
                    Variable.get(f"der_tab_{tab}_replace_flag_psg",default_var="None",
                    )
                    not in ("None", "<NA>")
                )
            ):
                raise AirflowFailException(
                    f"""
                    ERROR: Incorrect configuration for {tab} in derived tables. The psg_replace flag should be null.
                """
                )

            # check if the task is scheduled to run today
            base = datetime.now() - timedelta(days=1)

            iterator = croniter(
                Variable.get(f"der_tab_{tab}_schedule_interval"),
                base,
            )

            while True:
                if iterator.get_current(datetime).date().strftime(
                    "%Y-%m-%d"
                ) < datetime.utcnow().strftime("%Y-%m-%d"):
                    iterator.get_next(datetime).date().strftime("%Y-%m-%d")
                else:
                    break

            if iterator .get_current(datetime).date().strftime(
                "%Y-%m-%d"
            ) != datetime.utcnow().strftime("%Y-%m-%d"):
                remove_because_not_scheduled.append(tab)
                continue
            
            # if historic ingestion
            if Variable.get("ingestion_type") == "non-periodic":
                Variable.set(
                    f"der_tab_{tab}_query", json_file[f"{tab}_historic"]
                )
            # if periodic ingestion
            else:
                Variable.set(f"der_tab_{tab}_query", json_file[f"{tab}"])


            if Variable.get(f"der_tab_{tab}_query", default_var="") == "":
                remove_because_no_query.append(tab)
        df = df[~df.index.isin(remove_because_not_scheduled)]
        df = df[~df.index.isin(remove_because_no_query)]
        der_tabs_names_lst = df.index.to_list()
        Variable.set("der_tabs_names_lst", der_tabs_names_lst)
        Variable.set("der_tabs_remove_because_no_query", remove_because_no_query)
        Variable.set("der_tabs_remove_because_not_scheduled", remove_because_not_scheduled)

        execution_order_dict = {}
        for order in sorted(set(df.execution_order.to_list())):
            execution_order_dict[order] = df[
                df["execution_order"] == order
            ].index.to_list()
        Variable.set("der_tabs_exec_order_dict", execution_order_dict)
        der_tab_configuration = {
            "der_tabs_names_list":der_tabs_names_lst,
            "der_tabs_exec_order_dict":execution_order_dict,
            "der_tabs_info_dict":der_tabs_info_dict,
            "der_tabs_copy_to_psg_job_position":der_tabs_copy_to_psg_job_position
        }

        overall_config = get_overall_config()
        overall_config["der_tab_config"] = der_tab_configuration
        Variable.set(
            "overall_config", 
            overall_config,
            serialize_json=True
        )
        

    except Exception as e:
        Variable.set("ERROR__derived_tables", e)
        Variable.set("der_tabs_names_lst", [])
        Variable.set("der_tabs_exec_order_dict", {})
        der_tab_configuration = {
            "der_tabs_names_list":[],
            "der_tabs_exec_order_dict":{},
            "der_tabs_info_dict":{},
            "der_tabs_copy_to_psg_job_position":None
        }
        overall_config = get_overall_config()
        overall_config["der_tab_config"] = der_tab_configuration
        Variable.set(
            "overall_config", 
            overall_config,
            serialize_json=True
        )
        raise AirflowFailException(f"Derived tables failed to start. Error: {e}")


def get_array_columns(entity):

    import pandas as pd
    from airflow.models import Variable

    db_engine = get_db_engine()
    generic_schema = pd.read_sql_table(
        f"{entity}_generic_schema_mapping",
        db_engine,
        schema=Variable.get("PGXSCHEMA"),
    )

    array_type_cols = generic_schema[
        generic_schema["generic_column_datatype"].str.contains("\[\]")
    ].generic_column_name.to_list()

    return array_type_cols


def check_if_to_run(task_name, condition):
    from airflow.models import Variable
    import pytz
    from datetime import date, datetime
    from google.cloud import bigquery
    from constants.constant import get_overall_config
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    table = wc._get_complete_table_name("fiscal_date_mapping", False)
    overall_config=get_overall_config()
    timezone = pytz.timezone(overall_config.get("ingestion_config")["dag_timezone"])
    now_datetime = datetime.now(timezone)
    today = now_datetime.strftime("%Y-%m-%d")
    query = (
        f"select {condition} from {table} where date = '{today}'"
    )
    rows = wc.execute_query(query, True)
    values = rows.iloc[0,0]
     # Normalize to a native Python bool so downstream usage and return type are consistent
    if hasattr(values, "item"):
        values = values.item()
    values = bool(values)
    # Sets True if today is 1st day of the fiscal week
    # else sets false
    overall_config["ingestion_config"][f"check_if_run_{task_name}"] = values
    Variable.set("overall_config", overall_config, serialize_json=True)
    return values



def get_preprocessing_queries(entity, module, schema_change=False):
    from google.cloud import storage
    from airflow.models import Variable
    import json
    client = storage.Client()
    bucket = client.get_bucket(Variable.get("gcs_bucket"))
    blob = bucket.get_blob(Variable.get("preprocessing_queries_loc"))
    file = json.loads(blob.download_as_text(encoding="utf-8"))

    if module == "fmt":
        if (
            Variable.get("ingestion_type") != "periodic"
            or schema_change
            or eval(Variable.get("model_refresh"))
        ):
            return file[f"{entity}_historic"]
        else:
            return file[f"{entity}_periodic"]
    else:
        if Variable.get("ingestion_type") != "periodic" or schema_change:
            return file[f"{entity}_historic"]
        else:
            return file[f"{entity}_periodic"]


async def download_csv_file_from_gcs(
    bucket_name: str, file_name: str, schema: dict
):
    """
    Download a CSV file from GCS bucket and convert it to a pandas dataframe.
    """
    import pandas as pd
    from io import BytesIO
    from google.cloud import storage

    # Initialize a GCS client
    storage_client = storage.Client()
    # Get the GCS bucket and file object
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    # Download the CSV file as bytes
    csv_bytes = blob.download_as_bytes()
    # Convert the CSV bytes to a pandas dataframe
    df = pd.read_csv(BytesIO(csv_bytes), converters=schema)
    return df


async def download_csv_files_from_gcs(
    bucket_name: str, file_names: list, schema: dict = {}
) -> list:
    """
    Download files from GCS bucket and convert them into pandas dataframe
    """

    import asyncio 
    # create a task for each of the file
    tasks = (
        download_csv_file_from_gcs(bucket_name, file_name, schema)
        for file_name in file_names
    )
    # await for all the files to download at once
    res = await asyncio.gather(*tasks)

    return res


async def delete_gcs_folder(bucket_name: str, folder_name: str) -> bool:
    """
    Delete a folder from the gcs bucket
    """
    
    from google.cloud import storage
    # Initialize a GCS client
    storage_client = storage.Client()
    try:
        # get the bucket
        bucket = storage_client.get_bucket(bucket_name)
        # get the blobs to delete
        blobs = list(bucket.list_blobs(prefix=folder_name))
        # delete the blobs
        bucket.delete_blobs(blobs)
        return True

    except Exception as E:
        print(f"DELETE GCS FOLDER ERROR : {E}")
        return False


def year_week_to_date(
    year_week,
    fiscal_calendar_table="fiscal_date_mapping",
    query_column="fiscal_year_week",
    fetch_column="date",
    agg_func="min",
):
    
    import pandas as pd
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    """
    Takes the year_week as input and give either the min date or the max date of that week as output.
    """
    if year_week:
        fiscal_calendar_table_name = wc._get_complete_table_name(fiscal_calendar_table, False)
        query = f"""
                    select {agg_func}({fetch_column})
                    from {fiscal_calendar_table_name}
                    where {query_column} = {year_week}
                """
        res = wc.execute_query(query, True)
        return str(res.iloc[0][0]).split(" ")[0]
    else:
        return None


def _get_task_summary(task_instance):
    """
    Function to push summary of a task to xcom

    @param task_instance: airflow.models.taskinstance.TaskInstance

    @return dict
        - Returns a dictionary with summary
    """
    task_summary = {
        "task_id": task_instance.task_id,
        "try_number": task_instance.try_number,
        "status": task_instance.state,
        "duration": task_instance.duration,
        "start_time": str(task_instance.start_date),
        "end_time": str(task_instance.end_date),
    }
    return task_summary


def log_dag_summary(**kwargs):
    from prettytable import PrettyTable
    task_instance = kwargs["ti"]
    dag_instance = task_instance.dag_run
    pt = PrettyTable()
    pt.field_names = [
        "task_id",
        "try_number",
        "status",
        "duration",
        "start_time",
        "end_time",
    ]
    _all_tasks = dag_instance.get_task_instances()
    for ti in _all_tasks:
        if ti.operator != "DummyOperator":
            _ti_summary = _get_task_summary(ti)
            pt.add_row(_ti_summary.values())
    pt.sortby = "start_time"
    log.info(pt)


def create_delta_table_bq(entity: str):
    
    import pandas as pd
    from airflow.models import Variable
    from airflow.exceptions import AirflowFailException
    from database_utility.utils import to_json, parse_json, except_distinct
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    to_json_string = to_json(warehouse)
    parse_json_string = parse_json(warehouse) 
    except_distinct_string = except_distinct(warehouse)

    new_schema_dict = eval(
        Variable.get(f"mapping_data_{entity}").replace("nan", "None")
    )
    new_schema = pd.DataFrame(
        {
            key: new_schema_dict[key]
            for key in [
                "generic_column_name",
                "generic_column_datatype",
                "required_in_product",
                "unique_by",
            ]
            if key in new_schema_dict
        }
    )
    new_schema_level = ",".join(
        set(
            new_schema[new_schema["unique_by"] == True]["generic_column_name"].to_list()
        )
    )
    new_schema = new_schema[new_schema["required_in_product"] == True]
    new_schema["inner_column"] = new_schema.apply(
        lambda x: f"{to_json_string}({x['generic_column_name']}) as {x['generic_column_name']}"
        if x["generic_column_datatype"] in ("varchar[]", "text[]")
        else x["generic_column_name"],
        axis=1,
    )
    new_schema["outer_column"] = new_schema.apply(
        lambda x: f"{parse_json_string}({x['generic_column_name']}) as {x['generic_column_name']}"
        if x["generic_column_datatype"] in ("varchar[]", "text[]")
        else x["generic_column_name"],
        axis=1,
    )
    try:
        prev_schema = wc._get_table_columns(f'{entity}_master') 
    except Exception as e:
        raise AirflowFailException(
            f"Failed to get previous schema for {entity}_master. Error: {e}"
        )
    new_schema_set = set(new_schema["generic_column_name"].to_list())
    prev_schema_set = set(prev_schema)

    # In case there is a change in column set between prev master table and new master table directly copy validated table
    if new_schema_set - prev_schema_set != set():
        raise AirflowFailException(
            f"{entity}_master in Warehouse missing columns from {entity}_generic_schema_mapping: {new_schema_set - prev_schema_set}\n Needs manual fix."
        )

    inner_columns_str = ",".join(set(new_schema["inner_column"].to_list()))
    outer_columns_str = ",".join(set(new_schema["outer_column"].to_list()))

    validated_table_name = wc._get_complete_table_name(f'{entity}_validated_table' , False)
    master_table_name = wc._get_complete_table_name(f'{entity}_master' , False)
    delta_table_name = wc._get_complete_table_name(f'{entity}_delta_table' , False)

    delta_table_query = f"""
        create or replace table {delta_table_name} as 
        (SELECT {outer_columns_str}, __record_type from (
        (SELECT
        {inner_columns_str},1 AS __record_type
        FROM
        {validated_table_name}
        {except_distinct_string} select  {inner_columns_str},1 as __record_type 
        from {master_table_name}) union all(
        SELECT 
        {inner_columns_str},0 AS __record_type from {master_table_name}
        where concat({new_schema_level}) not in (select concat({new_schema_level}) 
        from {validated_table_name})
        )))
    """
    Variable.set(f"{entity}_delta_query", delta_table_query)
    try:
        wc.execute_query(delta_table_query,False)
    except Exception as e:
        raise AirflowFailException(
            f"Exception in creating {entity}_delta_table. Error: {e}"
        )


def _generate_attribute_subquery(
    entity: str,
    attr: str,
    schema_level: str,
    refresh: bool,
    to_json_string: bool = False,
):
    from airflow.models import Variable
    from database_utility.utils import to_json, parse_json
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    """
    Private method to reuse generating the attributes query either from delta or validated table
    @param entity: str
        Mandatory string input i.e product or store
    @param attr: string
        Mandatory String of attribute name
    @param schema_level: string
        Mandatory string input for the schema level
    @param refresh: bool
        Mandatory boolean input
    @param to_json_string: bool
        @default False
        Optional boolean input to convert to json_string or normal string. with default
    Returns @string
    """
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    to_json_string_func = to_json(warehouse)
    parse_json_string_func = parse_json(warehouse) 

    if refresh:
        source_table = wc._get_complete_table_name(f"{entity}_validated_table", False)
        sub_query = (
            f"""
            select {schema_level}, '{attr}' as attribute_name, CASE 
            WHEN {attr} is NOT NULL THEN {to_json_string_func}({parse_json_string_func}({to_json_string_func}({attr}))) 
            ELSE NULL
            END AS attribute_value 
            from {source_table}
        """
            if to_json_string
            else f"""
            select {schema_level}, '{attr}' as attribute_name, CASE 
            WHEN {attr} is NOT NULL THEN cast({attr} as string) 
            ELSE NULL
            END AS attribute_value 
            from {source_table}
        """
        )
    else:
        source_table = wc._get_complete_table_name(f"{entity}_delta_table", False)
        sub_query = (
            f"""
            select {schema_level}, '{attr}' as attribute_name, CASE 
            WHEN {attr} is NOT NULL THEN {to_json_string_func}({attr}) 
            ELSE NULL
            END AS attribute_value,
            __record_type 
            from {source_table}
        """
            if to_json_string
            else f"""
            select {schema_level}, '{attr}' as attribute_name, CASE 
            WHEN {attr} is NOT NULL THEN cast({attr} as string) 
            ELSE NULL
            END AS attribute_value,
            __record_type  
            from {source_table}
        """
        )
    return sub_query


def _generate_attributes_query(
    refresh: bool,
    entity: str,
    schema_df,
    schema_level: str,
    attributes: set,
):
    import re
    from airflow.exceptions import AirflowFailException
    from database_utility.utils import complex_data_type
    from airflow.models import Variable
    """
    Private method to reuse generating the attributes query either from delta or validated table
    @param refresh: bool
        Mandatory boolean input
    @param entity: str
        Mandatory string input i.e product or store
    @param schema_df: Pandas dataframe
        Mandatory dataframe input containing column_name, data_type columns
    @param attributes: set
        Set of attributes that are part of the column_name
    Returns @string
    """
    attributes_sub_query = []
    warehouse = Variable.get("warehouse")
    complex_types = complex_data_type(warehouse)
    match_json_convertibles = re.compile("|".join(complex_types))

    for attr in attributes:
        try:
            datatype = schema_df[schema_df["column_name"] == attr][
                "data_type"
            ].to_list()[0]
        except Exception as e:
            raise AirflowFailException(f"Datatype not found for {attr}. Exception: {e}")
        matches = match_json_convertibles.search(datatype)
        sub_query = _generate_attribute_subquery(
            entity=entity,
            attr=attr,
            schema_level=schema_level,
            refresh=refresh,
            to_json_string=matches,
        )
        attributes_sub_query.append(sub_query)
    attributes_query = " UNION ALL ".join(attributes_sub_query)
    return attributes_query


def create_delta_attributes_table_bq(entity: str):

    import pandas as pd
    from pandas_gbq.gbq import GenericGBQException
    from airflow.models import Variable
    from google.cloud import bigquery
    from airflow.exceptions import AirflowFailException
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import except_distinct
    
    """
    Method to create delta for product attributes and store attributes table
    @param entity: str
        String input that takes table entity
    @return: None
    """

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    except_distinct_string = except_distinct(warehouse)
    # Fetch the previous schema
    try:
        prev_schema = wc._get_table_columns(f'{entity}_attributes') 
        prev_schema_set = set(prev_schema)
    except Exception as e:
        log.info(
            f"Failed due to exception while fetching previous schema: Error: {e}"
        )
        prev_schema_set = set()

    # Fetch the new schema
    schema_mapping_dict = eval(
        Variable.get(f"mapping_data_{entity}").replace("nan", "None")
    )
    schema_df = pd.DataFrame(
        {
            key: schema_mapping_dict[key]
            for key in [
                "generic_column_name",
                "generic_column_datatype",
                "is_pk",
                "is_attribute",
                "is_hierarchy",
                "required_in_product",
                "unique_by",
            ]
            if key in schema_mapping_dict
        }
    )

    # Fetch the level of unique ness
    new_schema_level_list = schema_df[schema_df["unique_by"] == True][
        "generic_column_name"
    ].to_list()
    new_schema_level = ",".join(new_schema_level_list)

    # attributes table will have unique_by fields, attribute name and attribute value
    new_schema_level_list.extend(["attribute_name", "attribute_value"])
    new_schema_set = set(new_schema_level_list)

    # Fetch the new attributes
    new_attributes_df = schema_df[schema_df["required_in_product"]]
    new_attributes_df = new_attributes_df[
        new_attributes_df["is_attribute"] | new_attributes_df["is_pk"]
    ]
    new_attributes_set = set(new_attributes_df["generic_column_name"].to_list())

    # New schema on bigquery for product validated table
    # new_schema_gbq_query = f"""
    #     select column_name as column_name, data_type as data_type from {Variable.get('gcp_project')}.{Variable.get('dataset')}.INFORMATION_SCHEMA.COLUMNS 
    #     where table_name = '{entity}_validated_table'
    # """
    # new_schema_gbq_df = pd.read_gbq(
    #     query=new_schema_gbq_query,
    #     project_id=f"{Variable.get('gcp_project')}",
    #     location=Variable.get("bigquery_region", default_var="US")
    # )
   
    new_schema_gbq_df = wc._get_table_columns_dtypes(f'{entity}_validated_table')

    if (
        (new_schema_set - prev_schema_set != set())
        or (prev_schema_set - new_schema_set != set())
        or Variable.get("ingestion_type") != "periodic"
    ):
        log.info(f"New_schema: {new_schema_set}")
        log.info(f"Prev schema: {prev_schema_set}")
        # Enter this if there is a change in structure of {entity}_attributes table
        log.warning(f"{entity}_attributes table schema has changed")
        # call historic product attributes refresh
        # bq_client = bigquery.Client(
        #     project=f"{Variable.get('gcp_project')}",
        #     location=Variable.get("bigquery_region", default_var="US")
        # )
        # job_config = bigquery.QueryJobConfig()
        # # Truncate existing delta table and re create
        # job_config.write_disposition = "WRITE_TRUNCATE"
        # job_config.destination = f"{Variable.get('gcp_project')}.{Variable.get('dataset')}.{entity}_attributes"

        # Note that in case of full refresh we are reading from {entity}_master
        new_attributes_query = _generate_attributes_query(
            refresh=True,
            entity=entity,
            schema_df=new_schema_gbq_df,
            schema_level=new_schema_level,
            attributes=new_attributes_set,
        )
        attributes_table = wc._get_complete_table_name(f'{entity}_attributes', False)
        new_attributes_query = f"create or replace table {attributes_table} as " + new_attributes_query
        Variable.set(
            f"{entity}_attributes_validated_query", new_attributes_query
        )
        # job = bq_client.query(
        #     Variable.get(f"{entity}_attributes_validated_query"),
        #     job_config=job_config,
        # )
        # job.result(job_retry=None)
        wc.execute_query(new_attributes_query , False)
        if Variable.get("ingestion_type") == "periodic":
            attributes_table = wc._get_complete_table_name(f'{entity}_attributes', False)
            attributes_delta_table = wc._get_complete_table_name(f'{entity}_attributes_delta_table', False)
            delta_query = f"""create or replace table {attributes_delta_table} as (select *, 1 as __record_type from {attributes_table})"""
            wc.execute_query(delta_query, False)
    else:
        # Enter this if there is no change in structure of {entity}_attributes table
        # Create attributes_validated table only based on delta master table
        log.info(f"{entity}_attributes schema has not changed. Generating delta")
        
        attributes_validated_table = wc._get_complete_table_name(f"{entity}_attributes_validated_table", False)
        # Note that in case of periodic we are reading from delta table.
        new_attributes_query = _generate_attributes_query(
            refresh=False,
            entity=entity,
            schema_df=new_schema_gbq_df,
            schema_level=new_schema_level,
            attributes=new_attributes_set,
        )
        new_attributes_query = f"create or replace table {attributes_validated_table} as " + new_attributes_query
        
        Variable.set(
            f"{entity}_attributes_validated_query", new_attributes_query
        )

        try:

            wc.execute_query(new_attributes_query, False)
        except Exception as e:
            raise AirflowFailException(
                f"Exception in creating {entity}_attributes_validated_table. Error: {e}"
            )

        attributes_delta_table = wc._get_complete_table_name(f'{entity}_attributes_delta_table', False)
        attributes_table = wc._get_complete_table_name(f'{entity}_attributes', False)
        attributes_delta_query = f"""
            create or replace table {attributes_delta_table} as (select {new_schema_level}, attribute_name, attribute_value, __record_type from 
            {attributes_validated_table}
            {except_distinct_string} select {new_schema_level}, attribute_name, attribute_value, 1 as __record_type 
            from {attributes_table})
        """
        Variable.set(f"{entity}_attributes_delta_query", attributes_delta_query)

        try:
            wc.execute_query(attributes_delta_query, False)
        except Exception as e:
            raise AirflowFailException(
                f"Exception in creating {entity}_attributes_delta_table. Error: {e}"
            )

        try:
            refresh_query = _generate_attributes_query(
                refresh=True,
                entity=entity,
                schema_df=new_schema_gbq_df,
                schema_level=new_schema_level,
                attributes=new_attributes_set,
            )

            attributes_table = wc._get_complete_table_name(f'{entity}_attributes', False)
            refresh_query = f"create or replace table {attributes_table} as " + refresh_query

            Variable.set(f"{entity}_refresh_query", refresh_query)
            wc.execute_query(refresh_query, False)
        except Exception as e:
            Variable.set(f"{entity}_refresh_error", str(e))
            raise AirflowFailException(
                f"Updating BQ table with delta attributes failed for entity {entity}. Error:"
            )


class LongRunningTaskSensor:

    def __init__(self, timeout_hours=10, poke_interval=3600, **kwargs):
        self.timeout_hours = timeout_hours
        self.poke_interval = poke_interval
        self.context = kwargs

    def poke(self, context):

        from tasks import slack_integration as sl
        from airflow.utils.dates import timezone
        from airflow.models import Variable
        dag_start_time = context["dag_run"].start_date
        elapsed_time_hours = (timezone.utcnow() - dag_start_time).total_seconds() / 3600
        Variable.set("elasped_time", elapsed_time_hours)
        Variable.set("dag_start_time", dag_start_time)

        tasks_running_queued = [
            ti
            for ti in context["dag_run"].get_task_instances()
            if ti.state in {"running", "queued", "scheduled","up_for_retry","up_for_reschedule"}
            and ti.task_id != "check_long_running_task"
        ]
        if elapsed_time_hours >= self.timeout_hours or not tasks_running_queued:
            return True

        long_running_tasks = []

        for ti in tasks_running_queued:
            start_time = ti.start_date
            if start_time is None:
                continue
            end_time = timezone.utcnow()
            time_difference = (end_time - start_time).total_seconds() / 60

            if time_difference > 10:
                long_running_tasks.append(ti)

        if long_running_tasks:
            # send notification
            slack_client = sl.slack_notification(
                project_name=Variable.get("gcp_project"),
                slack_secret_name="slack_notification_token",
            )
            nl = "\n -"
            base_text = f"""Attention <!channel>, Below tasks of *{Variable.get("tenant")} {Variable.get("pipeline")}* pipeline are active from more than 10 mins :warning: \n"""
            for task in long_running_tasks:
                base_text += f"\n{task.task_id} -> {task.state}"
            message = {
                "text": base_text
            }
            slack_client.send_slack_message(message, thread_ts="")

        return False

    def poll(self):
        import time
        time.sleep(5)
        while not self.poke(self.context):
            time.sleep(self.poke_interval)
        return True


def long_task_sensor(timeout_hours, poke_interval, **kwargs):
    sensor = LongRunningTaskSensor(timeout_hours, poke_interval, **kwargs)
    return sensor.poll()


def warehouse_summary(**context):

    import pandas as pd
    from tasks import slack_integration as sl
    from google.cloud import bigquery, storage
    from airflow.models import Variable
    from google.cloud.exceptions import NotFound
    from queries.query_builder import build_slots_query
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import generate_create_table_ddl

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    dag_run_id = context["dag_run"].run_id
    dag_start_time = context["dag_run"].execution_date
    _all_tasks = context["dag_run"].get_task_instances()
    bq_operator_list = []
    for ti in _all_tasks:
        if ti.operator in  ("BigQueryExecuteQueryOperator","BigQueryOperator","GenericDataWarehouseOperator"):
            task_jb_id = ti.xcom_pull(key="job_id", task_ids=ti.task_id)
            if task_jb_id == None:
                continue
            elif task_jb_id.startswith("airflow"):
                corrected_task_jb_id = task_jb_id
            elif task_jb_id.startswith("["):
                corrected_task_jb_id = eval(task_jb_id)[0]
            else:
                continue
            bq_operator_list.append(
                {"task_id": ti.task_id, "job_id": corrected_task_jb_id}
            )

    Variable.set("bq_operator_list", bq_operator_list)
    df = pd.DataFrame.from_records(bq_operator_list)
    df["dag"] = "data_ingestion"
    df["dag_run_id"] = dag_run_id
    bq_jobs = df["job_id"].to_list()
    bq_jobs_str = ",".join([f'"{i}"' for i in bq_jobs])
    slots_query = build_slots_query(bq_jobs_str, dag_start_time, region=Variable.get('bigquery_region'))    
    slots_df = wc.execute_query(slots_query, True)
    Variable.set("bq_slots_df", slots_df)
    Variable.set("bq_airflow_df", df)
    slots_df_result = pd.merge(df, slots_df, on="job_id", how='inner', validate='one_to_one')
    Variable.set("warehouse_summary_table", slots_df_result)

    client = storage.Client()
    bucket = client.get_bucket(Variable.get("gcs_bucket"))
    blob = bucket.blob(
        f"{Variable.get('tenant_alias')}/warehouse_summary/warehouse_summary_{dag_run_id}"
    )
    blob.upload_from_string(slots_df_result.to_csv(index=False))

    try:
        wc.table_exists("pipeline_warehouse_summary", True)
    except NotFound:
        table_schema =  {
                    "task_id": "string",
                    "job_id": "string",
                    "dag": "string",
                    "dag_run_id": "string",
                    "date": "timestamp",  
                    "user_email": "string",
                    "Project_id": "string",
                    "Job_Start_Time": "string",
                    "Job_End_Time": "string",
                    "Time_Taken_InSeconds": "int",  
                    "total_slot_ms": "float",  
                    "total_bytes_processed": "int", 
                    "total_slot_cost": "string"
                    }
        query = generate_create_table_ddl(warehouse, wc._get_complete_table_name("pipeline_warehouse_summary", False), table_schema, partition_by="timestamp_trunc(date, day)")
        wc.execute_query(query, False)

    wc.append_to_table_using_df(slots_df_result, "pipeline_warehouse_summary")

    link = f"https://console.cloud.google.com/storage/browser/_details/{Variable.get('gcs_bucket')}/{Variable.get('tenant_alias')}/warehouse_summary/warehouse_summary_{dag_run_id}"
    link = link.replace("+", "%2B")
    slack_client = sl.slack_notification(
        project_name=Variable.get("gcp_project"),
        slack_secret_name="slack_notification_token",
    )
    message = {
        "text": f"""Attention <!channel>, BigQuery summary is created, please visit {link}. """
    }
    slack_client.send_slack_message(message, thread_ts="")

    return True


def run_db_query(query):

    import psycopg2
    import pandas as pd
    from airflow.models import Variable

    try:
        user = Variable.get("PGXUSER")
        password = Variable.get("PGXPASSWORD")
        host = Variable.get("PGXHOST")
        port = Variable.get("PGXPORT")
        database = Variable.get("PGXDATABASE")

        with psycopg2.connect(
            user=user, password=password, host=host, port=port, database=database
        ) as conn:
            result = pd.read_sql(query, conn)
            return result

    except (Exception) as error:
        log.error("Error while connecting to PostgreSQL:", error)

def async_to_sync(awaitable):
    import asyncio
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(awaitable)

def connect_to_db_and_execute_query(
    query,
):
    import pandas as pd
    db_engine = get_db_engine()
    result = pd.read_sql(query, db_engine)
    return result


def generate_mp_bucket_code():
    
    import math
    import pandas_gbq
    import pandas as pd
    from google.cloud import bigquery
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from queries.query_builder import build_mp_bucket_code_historic_query, build_mp_bucket_code_periodic_query

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    """
    Generates mp_bucket_code for product_validated_table
    """
    bqclient = bigquery.Client(
        project=Variable.get("gcp_project"),
        location=Variable.get("bigquery_region", default_var="US")
    )
    table_name = wc._get_complete_table_name("product_validated_table", False)
    bckp_table_name = wc._get_complete_table_name("product_validated_table_with_periodic_lookup", False)
    table_cols = wc._get_table_columns("product_validated_table")
    
    # If mp_bucket_column doesn't exist, it gets added with default value of 1
    if "mp_bucket_code" not in table_cols:
        log.info(f"Column 'mp_bucket_code' doesnt exist in the {table_name}. Adding mp_bucket_code with default value 1")
        query = f"""CREATE OR REPLACE TABLE {table_name} AS SELECT *, CAST(1 AS STRING) AS mp_bucket_code FROM {table_name}"""
        log.info(query)
        wc.execute_query(query, False)
    
    pseudo_hierarchy_level = Variable.get("pseudo_hierarchy_level", None)
    hierarchies = None
    if pseudo_hierarchy_level and pseudo_hierarchy_level!="None":
        log.info(f"Provided pseudo_hierarchy_level {pseudo_hierarchy_level}")
        hierarchies = pseudo_hierarchy_level
    else:
        log.info(f"pseudo_hierarchy_level {pseudo_hierarchy_level} is not provided")
        mapping_data_df = pd.DataFrame(eval(Variable.get("mapping_data_product",default_var="{}").replace('nan','None')\
                                            ))
        hierarchy_df = mapping_data_df[mapping_data_df['is_hierarchy']][['generic_column_name','hierarchy_level']]
        article_hierarchy_level = hierarchy_df[hierarchy_df['generic_column_name']=='article']['hierarchy_level'].tolist()[0]
        hierarchies = ",".join(sorted(hierarchy_df[hierarchy_df['hierarchy_level']<article_hierarchy_level]['generic_column_name'].tolist()))
    
    log.info(f"Hierarchies : {hierarchies}")

    num_products_per_bucket = int(Variable.get("num_products_per_mp_bucket_code", default_var="200"))
    log.info(f"Number of products per bucket {num_products_per_bucket}")

    join_conditions = [f"T1.{hierarchy}=T2.{hierarchy}" for hierarchy in hierarchies.split(",")]
    window_partition = ",".join(f'T1.{hierarchy}' for hierarchy in hierarchies.split(","))
    
    if Variable.get("ingestion_type").lower() != "periodic":
        query = build_mp_bucket_code_historic_query(table_name, hierarchies, num_products_per_bucket, window_partition, " AND ".join(join_conditions))
        log.info(f"Recreating the {table_name} with mp_bucket_code\n{query}")
        wc.execute_query(query, False)
        log.info(f"Successfully created the {table_name} with mp_bucket_code")
    
    else:
        query = f"""
            CREATE TABLE IF NOT EXISTS {bckp_table_name} AS
            SELECT
                T.*, CAST(0 AS INTEGER) AS num_products
            FROM {table_name} T
            LIMIT 0
        """
        log.info(f"Creating table {bckp_table_name} if it doesnt exist\n{query}")
        wc.execute_query(query, False)
        query = build_mp_bucket_code_periodic_query(table_name, bckp_table_name, hierarchies, num_products_per_bucket, window_partition, " AND ".join(join_conditions))
        log.info(f"Recreating the {table_name} with mp_bucket_code\n{query}")
        wc.execute_query(query, False)
        log.info(f"Successfully created the {table_name} with mp_bucket_code")
    

    # Creating table for periodic lookup
    log.info(f"Creating {table_name}_periodic_lookup")
    query = f"""
        CREATE OR REPLACE TABLE {table_name}_periodic_lookup AS
        WITH num_products_per_hierarchy AS
        (
            SELECT
                {hierarchies},
                COUNT(distinct product_code) AS num_products
            FROM {table_name}
            GROUP BY {hierarchies}
        )
        SELECT
            T1.*,
            T2.num_products
        FROM {table_name} T1
        JOIN num_products_per_hierarchy T2
        ON {" AND ".join(join_conditions)}
    """
    wc.execute_query(query, False)
    log.info(f"Successfully created the table {table_name}_periodic_lookup")

def invalidate_cache(status: str):
    from airflow.models import Variable
    from tasks.utils import idtoken_from_metadata_server
    from tasks.api_utils import REDIS_CACHE_INVALIDATION, HTTPS, DEPLOY_ENV_MAPPING, call_http_endpoint
    if status not in {"completed", "failed", "in_progress"}:
        raise ValueError(f"Invalid status {status}. Valid values are 'completed', 'failed', 'in_progress'")
    tenant = Variable.get("tenant")
    deploy_env = Variable.get("pipeline")
    target_url = f"{HTTPS}{tenant}{DEPLOY_ENV_MAPPING[deploy_env].value}.impactsmartsuite.com/{REDIS_CACHE_INVALIDATION}"
    headers = {
        "Authorization": f"Bearer {idtoken_from_metadata_server(target_url)}",
        "Content-Type": "application/json"
    }
    payload = {"status": status}
    call_http_endpoint(url=target_url, method='POST', payload=payload, headers=headers)


def get_update_query_derived_table(table_name, gb_table_name):

    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from airflow import models

    warehouse = models.Variable.get("warehouse")
    warehouse_kwargs = models.Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    gb_table_name_complete = wc._get_complete_table_name(gb_table_name, False)
    update_main_table_query_gbq = f"""
            select * from {gb_table_name_complete}
        """
    models.Variable.set(f"update_main_table_query_gbq_{table_name}", update_main_table_query_gbq)