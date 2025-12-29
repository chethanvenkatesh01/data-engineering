def lost_sales_imputation_query():
    from airflow.models import Variable

    impute_features = eval(Variable.get("impute_features"))
    lsi_channels = eval(Variable.get("lsi_channel"))
    active_stores_cutoff=eval(Variable.get('active_stores_cutoff'))
    sell_through_cutoff=eval(Variable.get('sell_through_cutoff'))
    impute_conditions_query = []
    imputed_query = []
    final_table_query = []
    qty_column = impute_features["qty"][0]
    inv_column = impute_features["inventory"][0]
    impute_features.pop("qty")
    impute_features.pop("inventory")

    multi_channel_thresold_query=[]
    for i in lsi_channels:
        multi_channel_thresold_query.append(f"""when oos_flag  = 1 and concat({' , "___" , '.join(eval(Variable.get("level_of_store_clustering")))}) in ('{i}') and  active_stores_ratio >={active_stores_cutoff[i]} and 
        sell_through_ratio >={sell_through_cutoff[i]} and {qty_column} > {qty_column}_first_imputed 
        then round({qty_column},0) 
        when oos_flag  = 1 and concat({' , "___" , '.join(eval(Variable.get("level_of_store_clustering")))}) in ('{i}') and  active_stores_ratio >={active_stores_cutoff[i]} and 
        sell_through_ratio >= {sell_through_cutoff[i]} and {qty_column} < {qty_column}_first_imputed 
        then round({qty_column}_first_imputed,0)""")

    multi_channel_thresold_query= "when false then 0 " if len(multi_channel_thresold_query) < 1 else multi_channel_thresold_query
    for k, v in impute_features.items():
        if not (v):
            continue
        for i in v:
            if k == "price":
                impute_conditions_query.append(
                    f"coalesce(avg(case when {qty_column} =0 then null else {i} end) over(partition by {' , '.join(eval(Variable.get('level_of_store_clustering')))}, cluster_name , product_code , date ), \
                      avg(case when {qty_column} =0 then null else {i} end) over(partition by {' , '.join(eval(Variable.get('level_of_store_clustering')))},cluster_name , product_code , fiscal_year_week ),\
                     avg(case when {qty_column} =0 then null else {i} end) over(partition by  {' , '.join(eval(Variable.get('level_of_store_clustering')))}, product_code, fiscal_year_week )) as {i}_first_imputed"
                )
                imputed_query.append(
                    f"(case when {i} is null then round({i}_first_imputed,2) else {i} end) as {i}_imputed "
                )
                final_table_query.append(f"{i}_first_imputed")

            else:
                impute_conditions_query.append(
                    f"coalesce(avg({i}) over(partition by {' , '.join(eval(Variable.get('level_of_store_clustering')))}, cluster_name , product_code , date ),avg({i}) over(partition by {' , '.join(eval(Variable.get('level_of_store_clustering')))}, cluster_name , product_code , fiscal_year_week ),avg({i}) over(partition by {' , '.join(eval(Variable.get('level_of_store_clustering')))},  product_code, fiscal_year_week )) as {i}_first_imputed"
                )
                imputed_query.append(
                    f"(case when {i} is null then round({i}_first_imputed,2) else {i} end) as {i}_imputed "
                )
                final_table_query.append(f"{i}_first_imputed")

    impute_conditions_query_const = ",".join(impute_conditions_query)
    imputed_query_const = ",".join(imputed_query)
    final_table_query_const = ",".join(final_table_query)
    on_condition_sc= [f" a.{i} = b.{i}" for i in eval(Variable.get("level_of_store_clustering"))]
    temp_table_cols= [f"a.{i}" for i in eval(Variable.get("level_of_store_clustering"))]

    base_table_query = f"select *, avg(case when {inv_column} <= 0 then null else {qty_column} end ) over(partition by {' , '.join(eval(Variable.get('level_of_store_clustering')))}, cluster_name , product_code , date) as {qty_column}_first_imputed, {impute_conditions_query_const}"

    where_backfill= '' if Variable.get("ingestion_type") != 'periodic' or eval(Variable.get("main_master_schema_change")) else Variable.get("main_master_backfill")
    final_query = f"""
                        create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_table_imputed_interim as
                        with impute_conditions as(
                        select *, coalesce(round(safe_divide(active_stores,total_stores_cluster),2),0) as active_stores_ratio,  coalesce(round (safe_divide(weekly_qty,max(date_inv) OVER (PARTITION BY {' , '.join(eval(Variable.get('level_of_store_clustering')))}, cluster_name, product_code, fiscal_year_week)),2),0) as sell_through_ratio 
                        from (select a.*,b.cluster_name,total_stores_cluster,
                        (case when {inv_column} =0 then 1 else 0 end) as oos_flag,count(distinct(case when {qty_column}  = 0 then null else a.store_code end )) over (partition by {','.join(temp_table_cols)}, b.cluster_name,product_code,fiscal_year_week) as active_stores,
                        sum({qty_column}) over (partition by {','.join(temp_table_cols)}, b.cluster_name,product_code,fiscal_year_week) as weekly_qty, sum({inv_column}) over (partition by {','.join(temp_table_cols)}, b.cluster_name,product_code,date) as date_inv 
                        from  {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_table  a
                        left join  (select cluster_name,store_code,{' , '.join(eval(Variable.get('level_of_store_clustering')))},count(distinct store_code ) over(partition by {' , '.join(eval(Variable.get('level_of_store_clustering')))}, cluster_name) as total_stores_cluster 
                        from {Variable.get('gcp_project')}.{Variable.get('dataset')}.store_clustering)  b 
                        on a. store_code=b.store_code and {' and '.join(on_condition_sc)}
                         where date> current_date()-{where_backfill}
                        )
                        ),

                    base_table as (
                        {base_table_query} from impute_conditions
                    ),

                    imputed_table as (
                        select *,case {' '.join(multi_channel_thresold_query)} else {qty_column} end as {qty_column}_imputed ,{imputed_query_const} from base_table ),

                    final_table as (
                        select *except({qty_column}_first_imputed,active_stores_ratio,sell_through_ratio,{final_table_query_const}),
                        (case when {qty_column}={qty_column}_imputed then 0 else 1 end) as imputation_flag
                        from imputed_table
                    )

                    select * from final_table
                    """
    return final_query


def price_imputation_query():

    import pandas as pd
    from airflow.models import Variable

    hierarchy_list = eval(Variable.get("all_hierarchies_reverse"))
    hierarchy_list.remove("product_code")
    impute_features = eval(Variable.get("impute_features"))
    qty_column = impute_features["qty"][0]
    inv_column = impute_features["inventory"][0]
    impute_features.pop("qty")
    impute_features.pop("inventory")
    on_condition_sc= [f" a.{i} = b.{i}" for i in eval(Variable.get("level_of_store_clustering"))]
    temp_name = []
    temp_table = []
    discount_imputed_list = []
    join_query = []
    for j in hierarchy_list:
        temp_day = []
        temp_week = []
        for i in impute_features["discount"]:
            temp_day.append(
                f"safe_divide(sum({i}_imputed*{inv_column}),sum({inv_column})) as {i}_{j}_day"
            )
            temp_week.append(
                f"safe_divide(sum({i}_imputed*{inv_column}) ,sum({inv_column})) as {i}_{j}_week"
            )

        temp_day_const = ",".join(temp_day)
        temp_week_const = ",".join(temp_week)
        temp_table.append(
            f"""temp_{j} as (select a.*,b.*except({j},fiscal_year_week,{' , '.join(eval(Variable.get('level_of_store_clustering')))})
                                from
                                ((select {j},{' , '.join(eval(Variable.get('level_of_store_clustering')))},date,fiscal_year_week,{temp_day_const}
                                from base_table group by {j},{' , '.join(eval(Variable.get('level_of_store_clustering')))},date,fiscal_year_week) a
                                left join 
                                (select {j},{' , '.join(eval(Variable.get('level_of_store_clustering')))},fiscal_year_week,{temp_week_const}
                                from base_table group by {j},{' , '.join(eval(Variable.get('level_of_store_clustering')))},fiscal_year_week ) b
                                on a.{j}=b.{j} and a.fiscal_year_week=b.fiscal_year_week and {' and '.join(on_condition_sc)})) """
        )
        on_condition_temp= [f" a.{i} = {j}.{i}" for i in eval(Variable.get("level_of_store_clustering"))]
        join_query.append(
            f"""left join temp_{j} as {j}
                             on a.{j}={j}.{j} and a.date={j}.date and {' and '.join(on_condition_temp)}  """
        )

    for i in impute_features["discount"]:
        temp = []
        discount_imputed_list.append(f"{i}_imputed")
        temp.append(f"coalesce ( {i}_imputed")
        for j in hierarchy_list:
            temp.append(f"{i}_{j}_day , {i}_{j}_week")
        temp.append(f"NULL) as {i}_imputed")
        temp_name.append(temp)

    final_select = []

    for i in temp_name:
        final_select.append(",".join(i))

    final_select_const = ",".join(final_select)
    discount_imputed_list_const = ",".join(discount_imputed_list)
    temp_table_const = ",".join(temp_table)
    join_quer_const = " ".join(join_query)

    price_imputed_list = [f"{i}_imputed" for i in impute_features["price"]]
    price_imputed_list_const = ",".join(price_imputed_list)    

    final_impute_query = []
    for i in impute_features["price"]:
        final_impute_query.append(
            f"coalesce({i}_imputed,LAST_VALUE({i}_imputed IGNORE NULLS) OVER(partition by product_code,{' , '.join(eval(Variable.get('level_of_store_clustering')))} ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),FIRST_VALUE({i}_imputed IGNORE NULLS) OVER(partition by product_code,{' , '.join(eval(Variable.get('level_of_store_clustering')))} ORDER BY date ROWS BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)) as {i}_imputed"
        )

    final_impute_query_const = ",".join(final_impute_query)

    master_table_imputed_partition_column = 'date'
    if Variable.get("ingestion_type") != 'periodic' or eval(Variable.get("main_master_schema_change")):
        create_or_insert_query_start_validated= f"""
            create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_input_imputed_validated_table
            partition by {master_table_imputed_partition_column} as"""
        create_or_insert_query_end_validated=''
        create_main_table=f""" create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_input_imputed 
                            partition by {master_table_imputed_partition_column}
                            as
                            (
                                select * from {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_input_imputed_validated_table
                            )"""

    else: 
        master_cols_query = f"""SELECT column_name
                FROM {Variable.get('gcp_project')}.{Variable.get('dataset')}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = 'master_input_imputed' """
        master_cols_df = pd.read_gbq(
            master_cols_query,
            project_id=Variable.get('gcp_project'),
            dialect='standard',
            location=Variable.get("bigquery_region", default_var="US")
        )
        master_table_column_names = list(master_cols_df["column_name"])
        create_or_insert_query_start_validated= f"""
            create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_input_imputed_validated_table
            partition by {master_table_imputed_partition_column} as"""
        create_or_insert_query_end_validated=''
        create_main_table=f"""
                    BEGIN
                    BEGIN TRANSACTION;
                    delete from {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_input_imputed where date > current_date()-{Variable.get("main_master_backfill")};
                    insert into {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_input_imputed ({','.join(master_table_column_names)})
                    select {','.join(master_table_column_names)} from 
                       (
                        select * from {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_input_imputed_validated_table
                            );
                    COMMIT TRANSACTION;
                    EXCEPTION WHEN ERROR THEN
                    SELECT @@error.message;
                    ROLLBACK TRANSACTION;
                    RAISE USING message = FORMAT("error found: %s.", @@error.message);
                    END;"""

    impute_query = f""" 
    {create_or_insert_query_start_validated}
    ( with base_table as (
        select *
        from {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_table_imputed_interim
    ),
     {temp_table_const},
    
    coalesce_discount_table as (
        select a.*except({discount_imputed_list_const}),{final_select_const}
        from base_table as a
        {join_quer_const}
    ),
    price_imputation as (
        select a.*except({price_imputed_list_const}),{Variable.get("price_impute_string")}
        from coalesce_discount_table a
        left join {Variable.get('gcp_project')}.{Variable.get('dataset')}.product_master b
        on a. product_code=b. product_code
    ),
    final_imputation as (
        select *except({price_imputed_list_const}), {final_impute_query_const}
        from price_imputation
    )
    
    select *except({inv_column}), (case when {inv_column}<={qty_column}_imputed then {qty_column}_imputed else {inv_column} end) as {inv_column}  ,({qty_column}_imputed-{qty_column}) as lost_sales from final_imputation
    )
    {create_or_insert_query_end_validated}
    """

    impute_query_price = f""" 
    {create_or_insert_query_start_validated}
    (
    with base_table as (
        select *
        from {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_table_imputed_interim 
    ),
    
    price_imputation as (
        select a.*except({price_imputed_list_const}),{Variable.get("price_impute_string")}
        from base_table a
        left join {Variable.get('gcp_project')}.{Variable.get('dataset')}.product_master b
        on a. product_code=b. product_code
    ),
    final_imputation as (
        select *except({price_imputed_list_const}), {final_impute_query_const}
        from price_imputation
    )
    
    select *, max({inv_column}) over(partition by product_code,store_code,fiscal_year_week) as max_week_inventory
    from
    (select *except({inv_column}), (case when {inv_column}<={qty_column}_imputed then {qty_column}_imputed else {inv_column} end) as {inv_column}, ({qty_column}_imputed-{qty_column}) as lost_sales  from final_imputation)
    )
    {create_or_insert_query_end_validated}
    """

    main_impute_query=f"""
                    {create_main_table}
    """
    return impute_query_price if impute_features["discount"] == [] else impute_query, main_impute_query
