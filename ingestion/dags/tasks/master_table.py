


def generate_query():
    import urllib
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    from airflow.exceptions import AirflowFailException
    from tasks.utils import get_impute_features, get_preprocessing_queries
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import generate_create_table_ddl_using_select
    from queries.query_builder import build_master_table_transaction

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

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

    if not eval(Variable.get('override_master_table_query')):

        get_impute_features()

        cols_data = pd.read_sql(
            f"""select concat('{{ ', string_agg(temp,' , ') ,' }}') as col from (select concat( '"',source,'"',  ' : ' , '"' ,STRING_AGG(gbq_formula, ' , '), '"')  as temp from {Variable.get('PGXSCHEMA')}.feature_metadata 
                                        where entity in ('master_table') group by source ) as a""",
            db_engine,
        )

        fmd = pd.read_sql(
            f"""select * from {Variable.get('PGXSCHEMA')}.feature_metadata 
                    where entity in ('master_table')""",
            db_engine,
        )

        # As per recommendation by DB Team we are mandating that these columns 
        # be present in master table for clustering purpose
        # Refer to comments on https://impactanalytics.atlassian.net/browse/BQ-103
        if Variable.get("ingestion_type") != 'periodic' or eval(Variable.get("main_master_schema_change")):
            if not (
                len(fmd[fmd["column_name"] == 'product_bucket_code']['column_name']) and
                len(fmd[fmd["column_name"] == 'channel']['column_name'])
            ):
                raise AirflowFailException("'product_bucket_code' and 'channel' columns should be mandatorily present in master_table for clustering the table.")

            __cluster_by_settings = fmd[
                fmd["column_name"] == "__cluster_by"
            ]

            # If no __cluster_by condition is configured, 
            # then we will take 'channel', 'product_bucket_code', 'sub_channel' (optional)
            # as clustering condition by default
            
            __cluster_by = __cluster_by_settings[
                __cluster_by_settings['column_name'] == '__cluster_by']['gbq_formula'].get(0, "channel, product_bucket_code")
            
            if any(fmd['column_name'].isin(['subchannel'])):
                __cluster_by += ", subchannel"

            Variable.set('master_table_clustered_on', __cluster_by)

        fmd = fmd[
            (fmd["column_name"] != "__cluster_by")
        ]

        step_query_cols = fmd[fmd["source"] == "master_table"].column_name.to_list()
        e_cols = fmd[fmd["source"] != "master_table"].column_name.to_list()
        e_tables = fmd[fmd["source"] != "master_table"].source.to_list()

        except_step_cols = []
        for c, t in zip(e_cols, e_tables):
            if c in step_query_cols:
                except_step_cols.append(f"{t.split('_')[0]}.{c}")

        cols_dict = eval(cols_data.col.iloc[0].replace("\n", ""))

        generic_master_mapping = pd.read_sql_table(
            "generic_master_mapping", db_engine, schema=Variable.get("PGXSCHEMA")
        )

        tables_and_mapping = generic_master_mapping[
            (generic_master_mapping["is_required_in_master"] == True)
            & (
                ~(
                    generic_master_mapping["destination_table"].isin(
                        ["inventory_master", "transaction_master"]
                    )
                )
            )
        ][["destination_table", "generic_mapping_table"]].copy()

        tables = tables_and_mapping.destination_table.to_list()
        all_tables = []
        for i in tables:
            all_tables.append(i.rsplit("_",1)[0])

        mapping_tables = tables_and_mapping.generic_mapping_table.to_list()

        inventory_mapping = pd.read_sql_table(
            "inventory_generic_schema_mapping",
            db_engine,
            schema=Variable.get("PGXSCHEMA"),
        )
        transaction_mapping = pd.read_sql_table(
            "transaction_generic_schema_mapping",
            db_engine,
            schema=Variable.get("PGXSCHEMA"),
        )

        inv_level = inventory_mapping[
            inventory_mapping["unique_by"] == True
        ].generic_column_name.to_list()
        trans_level = transaction_mapping[
            transaction_mapping["unique_by"] == True
        ].generic_column_name.to_list()

        inv_trans = list(set(inv_level) & set(trans_level))
        inv_trans_const = ",".join(inv_trans)
        inv_trans_on = []
        inv_trans_select = []

        for i in inv_trans:
            inv_trans_on.append(f"i.{i} = t.{i} ")
            inv_trans_select.append(f" coalesce(i.{i}, t.{i}) as {i} ")

        inv_trans_select_const = ",".join(inv_trans_select)
        inv_trans_on_const = " and ".join(inv_trans_on)

        sub_select = []
        sub_query = []
        for tab, mapp, dt in zip(all_tables, mapping_tables, tables):
            temp_map = pd.read_sql_table(
                mapp, db_engine, schema=Variable.get("PGXSCHEMA")
            )
            temp_level = temp_map[
                temp_map["unique_by"] == True
            ].generic_column_name.to_list()
            temp_level_const = ",".join(temp_level)

            temp_level_final_const = f"""{tab}.*except({temp_level_const} )"""
            temp_on = []
            for i in temp_level:
                temp_on.append(f" main.{i} = {tab}.{i} ")
            temp_on_const = " and ".join(temp_on)

            sub_select.append(temp_level_final_const)
            tab_name= "fiscal_date_mapping" if tab=="fiscal_date" else f"{tab}_master"
            sub_query.append(
                f"""
                    left join (select {temp_level_const} ,{cols_dict[dt]} from {Variable.get('gcp_project')}.{Variable.get('dataset')}.{tab_name} group by {temp_level_const})  as {tab}
                    on {temp_on_const}     
            """
            )

        sub_select_const = ",".join(sub_select)
        sub_query_const = " ".join(sub_query)

        impute_features = eval(Variable.get("impute_features"))
        qty_column = impute_features["qty"][0]
        inv_column = impute_features["inventory"][0]


    master_table_partition_column = 'date'
    
    if Variable.get("ingestion_type") != 'periodic' or eval(Variable.get("main_master_schema_change")):
        if eval(Variable.get('override_master_table_query')):
            Variable.set('master_table_clustered_on', "channel, product_bucket_code")
            master_validated_table_query=get_preprocessing_queries("master_table_query","master_table",eval(Variable.get("main_master_schema_change")))
        
        else:
            master_validated_table_query=f"""
            create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_validated_table  as (
            select *except({inv_column},{qty_column}), coalesce( (case when {inv_column} <{qty_column} then {qty_column} when {inv_column} <0 then 0 else {inv_column} end),{qty_column}) as {inv_column},
                                                    coalesce({qty_column},0) as {qty_column}
                            from 
                            (
                            select * {", " + cols_dict['master_table'] if "master_table" in cols_dict.keys() else ""}
                            from
                            (select main.*,{sub_select_const} 
                            from
                            (select {inv_trans_select_const},i.*except({inv_trans_const}), t.*except({inv_trans_const})
                            FROM
                                (select {inv_trans_const},{cols_dict['inventory_master']} from {Variable.get('gcp_project')}.{Variable.get('dataset')}.inventory_master group by {inv_trans_const}) i
                                FULL OUTER JOIN 
                                (select {inv_trans_const},{cols_dict['transaction_master']}
                                from {Variable.get('gcp_project')}.{Variable.get('dataset')}.transaction_master
                                group by {inv_trans_const}
                                ) t
                                ON
                                ({inv_trans_on_const}) ) main
                                {sub_query_const})
                                )
                                )
                """
        master_table_name = wc._get_complete_table_name('master_table', False)
        master_validated_table_name = wc._get_complete_table_name('master_validated_table', False)
        master_query = generate_create_table_ddl_using_select(
            warehouse,
            master_table_name,
            f"select * from {master_validated_table_name}",
            partition_by=master_table_partition_column,
            clustered_by=Variable.get('master_table_clustered_on')
        )
    else:
        if eval(Variable.get('override_master_table_query')):
            master_validated_table_query=get_preprocessing_queries("master_table_query","master_table",eval(Variable.get("main_master_schema_change")))
        else:
            master_validated_table_query=f"""
            create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_validated_table 
             partition by {master_table_partition_column}
               as (
                    select *except({inv_column},{qty_column}), coalesce( (case when {inv_column} <{qty_column} then {qty_column} when {inv_column} <0 then 0 else {inv_column} end),{qty_column}) as {inv_column},
                                                    coalesce({qty_column},0) as {qty_column}
                            from 
                            (
                            select * {", " + cols_dict['master_table'] if "master_table" in cols_dict.keys() else ""}
                            from
                            (select main.*,{sub_select_const} 
                            from
                            (select {inv_trans_select_const},i.*except({inv_trans_const}), t.*except({inv_trans_const})
                            FROM
                                (select {inv_trans_const},{cols_dict['inventory_master']} from {Variable.get('gcp_project')}.{Variable.get('dataset')}.inventory_master 
                                 where date > current_date()-{Variable.get("main_master_backfill")}
                                group by {inv_trans_const}) i
                                FULL OUTER JOIN 
                                (select {inv_trans_const},{cols_dict['transaction_master']}
                                from {Variable.get('gcp_project')}.{Variable.get('dataset')}.transaction_master
                                where date > current_date()-{Variable.get("main_master_backfill")}
                                group by {inv_trans_const}
                                ) t
                                ON
                                ({inv_trans_on_const}) ) main
                                {sub_query_const})
                                )
                                )"""
            
            
        master_table_column_names = wc._get_table_columns('master_table')
        master_table_name = wc._get_complete_table_name('master_table', False)
        master_validated_table_name = wc._get_complete_table_name('master_validated_table', False)
        master_cols_const = ",".join(master_table_column_names)
        backfill = Variable.get("main_master_backfill")
        master_query = build_master_table_transaction(master_table_name, master_validated_table_name, master_cols_const, backfill)

    Variable.set("master_validated_table_query", master_validated_table_query)
    Variable.set("master_query", master_query)
