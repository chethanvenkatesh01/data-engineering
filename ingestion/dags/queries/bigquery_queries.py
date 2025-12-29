TRANSFORM_VALIDATE = """
            -- create the base validation table from intermediate table along with the required checks
            create or replace table {validated_table_name} as
                
                with 
                    transformed_table as (
                        select *except({except_cols_const}),{transformed_cols_final}
                        from (
                            select *, null as dummy,null as handle_except 
                            from {raw_table_name}
                            {where_filter_clause} 
                            )
                        ),
                                
                    name_type_update as (
                        select *except({old_cols}),{typecast_cols_const}
                        from transformed_table
                        ),
                    
                    null_check as (
                    select *,
                           case when ({null_cols_const}) then true else false end as null_check
                    from name_type_update
                    ),
                    
                    primary_key_check as(
                    select * except(primary_duplicate_rank,handle_except), 
                           case when primary_duplicate_rank > 1 then true else false end as primary_key_check
                    from
                        (select *,
                            row_number() over(partition by {level_cols_const} order by {level_cols_const}) as primary_duplicate_rank
                        from null_check)   
                    )

                select *
                from primary_key_check
                ;

                -- create the anomaly summary table with proper row counts
                create or replace table {anomaly_summary_table_name} as

                (select '{entity_type}' as table,
                       'validation' as module,
                       'null_check' as rule,
                       'null_check' as rule_display_name,
                       '' as rule_description,
                       'DELETE' as action,
                       count(*) as no_original_rows,
                       countif(null_check) as affected_rows,
                       safe_divide(countif(null_check),count(*)) as affected_rows_percent,
                       'Pass' as status,
                        CURRENT_DATETIME() as insertion_date

                from {validated_table_name})
                union all
                (select '{entity_type}' as table,
                       'validation' as module,
                       'primary_key_check' as rule,
                       'primary_key_check' as rule_display_name,
                       '' as rule_description,
                       'DELETE' as action,
                       count(*) as no_original_rows,
                       countif(primary_key_check) as affected_rows,
                       safe_divide(countif(primary_key_check),count(*)) as affected_rows_percent,
                       'Pass' as status,
                        CURRENT_DATETIME() as insertion_date
                from {validated_table_name})
                ;

                -- update the rule_description in for both null_check and primary_key_check 

                UPDATE
                {anomaly_summary_table_name}
                SET
                rule_description =
                CASE
                    WHEN rule = 'null_check' THEN ( 
                    SELECT CONCAT("[ ",STRING_AGG(CONCAT(" ",col_name, " - ", nulls_count, " "), "'"), " ]") 
                    FROM ( 
                        SELECT col_name, COUNT(1) nulls_count 
                        FROM ( 
                            SELECT * 
                            FROM {validated_table_name}) t, 
                            UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'"(\w+)":null')) col_name 
                            WHERE LOWER(col_name) IN ('{null_check_const}') 
                            GROUP BY col_name 
                            ) 
                    )
                    WHEN rule = 'primary_key_check' THEN '{level_cols_const}'
                    ELSE rule_description
                END,
                status=
                CASE 
                  WHEN affected_rows>0 then 'Fail'
                  ELSE 'Pass'
                END
                WHERE True;
                
                -- create the anomaly table and copy all the defected rows
                create or replace table {anomaly_table_name} as 

                select *
                    except(null_check, primary_key_check),
                    case when null_check=True then 'null_check'
                         when primary_key_check = True then 'primary_key_check' 
                         else null end as QC,
                    'DELETE' as action,
                    CURRENT_DATETIME() as insertion_date
                from {validated_table_name}
                where null_check is true or primary_key_check is true
                ; 

                -- delete the defected rows
                delete from {validated_table_name}
                where null_check is true or primary_key_check is true
                ;

                -- drop the null_check and primary_key_check columns
                ALTER TABLE {validated_table_name} 
                DROP COLUMN IF EXISTS null_check;
                ALTER TABLE {validated_table_name} 
                DROP COLUMN IF EXISTS primary_key_check;                
"""

ANOMALY_METRIC_QUERY = """
                                WITH main as 
                                    (select * from (select "value" as type ,cast(initial_rc as string) as initial_data,cast ((initial_rc-after_nullcheck_rc) as string) as null_data_loss, cast((after_nullcheck_rc-duplicate_level_check_rc) as string) as duplicate_level_data_loss, cast((initial_rc-duplicate_level_check_rc) as string) as total_loss
                                    from {validated_table_name}
                                    group by 1,2,3,4,5
                                    union all
                                    select "percentage" as type ,cast(initial_rc as string) as initial_data,cast(safe_divide((initial_rc-after_nullcheck_rc),initial_rc)*100 as string) as null_data_loss, cast(safe_divide((after_nullcheck_rc-duplicate_level_check_rc),initial_rc)*100 as string) as duplicate_level_data_loss,
                                    cast(safe_divide(abs(initial_rc-duplicate_level_check_rc),initial_rc)*100 as string) as total_loss 
                                    from {validated_table_name})
                                    group by 1,2,3,4,5
                                    union all
                                    select type , initial_data , null_data_loss, duplicate_level_data_loss, total_loss
                                    from
                                    (select "error_rc" as type , '' as initial_data , concat("[ " ,string_agg(concat(" ",col_name, " - " , nulls_count , " "), "'") , " ]") as null_data_loss, "[{level_cols_const}]" as duplicate_level_data_loss, '' as total_loss
                                    from
                                    (SELECT col_name, COUNT(1) nulls_count
                                    from
                                    (select * 
                                    from {raw_table_name}
                                    {where_filter_clause})  t,
                                    UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'"(\w+)":null')) col_name
                                    where lower(col_name) in ('{null_cols_const}')
                                    GROUP BY col_name ))
                                    group by 1,2,3,4,5),
                                defaults as (
                                    select "value" as type ,'0' as initial_data , '0' as null_data_loss, '0' as duplicate_level_data_loss, '0' as total_loss
                                    union all
                                    select "percentage" as type ,'0' as initial_data , '0' as null_data_loss, '0' as duplicate_level_data_loss, '0' as total_loss
                                    union all
                                    select "error_rc" as type , '' as initial_data , '' as null_data_loss, '' as duplicate_level_data_loss, '' as total_loss
                                )
                                SELECT * FROM main
                                UNION ALL
                                SELECT * FROM defaults
                                WHERE NOT EXISTS ( SELECT * FROM main)                       
                                """


MERGE_QUERY = """
            merge {destination_table} as target
            using {master_table_query} as source
            on {on_condition_const} {optimization_clause}
            {delete_query}
            when not matched by target
            then insert ({master_cols_const}) values ({source_master_cols_const})
            when matched 
            then update set {update_set_const}
            """
COPY_ANOMALY_DATA_TO_GCS = """
                    EXECUTE IMMEDIATE
                    '''
                    EXPORT DATA OPTIONS(
                                uri='{gcs_uri}',
                                format='CSV',
                                overwrite=true,
                                header=true,
                                field_delimiter='{anomaly_data_file_delimiter}') AS
                    select ''' || (
                    SELECT
                        STRING_AGG(CASE
                            WHEN data_type <> 'ARRAY<STRING>' THEN column_name
                            ELSE CONCAT('array_to_string(',column_name,', " | ") as ',column_name)
                            END
                        )
                    FROM
                        `{gcp_project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                    WHERE
                        table_name = '{et}_anomaly' and column_name not in ('initial_rc', 'after_nullcheck_rc', 'primary_duplicate_rank', 'duplicate_level_check_rc', 'insertion_date')) || 
                    ''' from {gcp_project}.{dataset}.{et}_anomaly'''
                """

MASTER_TABLE_TRANSCATION = """
                    BEGIN
                        BEGIN TRANSACTION;
                        delete from {master_table} where date > current_date()-{backfill} ;
                        insert into {master_table} ({master_cols_const})
                        select {master_cols_const}
                        from
                        (
                            select * from {master_validated_table}
                        );
                        COMMIT TRANSACTION;
                        EXCEPTION WHEN ERROR THEN
                        SELECT @@error.message;
                        ROLLBACK TRANSACTION;
                        RAISE USING message = FORMAT("error found: %s.", @@error.message);
                        END;
                    """

LSI_QUERY_MV = """
        CREATE OR REPLACE MATERIALIZED VIEW {master_table_imputed}
        PARTITION BY date
        AS (
        select * from {master_table}
        );
    """


SLOTS_QUERY = """
        SELECT
                current_timestamp() as date,
                Job_Id as job_id,
                user_email,
                Project_id as project_id,
                FORMAT_TIMESTAMP("%c",Start_Time, "UTC") as job_start_time,
                FORMAT_TIMESTAMP("%c",End_Time, "UTC") as job_end_time,
                Timestamp_diff(End_Time,Start_Time,Second) as time_taken_inseconds,
                SUM(total_slot_ms) AS total_slot_ms,
                SUM(total_bytes_processed) AS total_bytes_processed,
                concat(cast(round(SUM(total_slot_ms)*0.06/3600000,2) as string),'$')  AS total_slot_cost,
                FROM
                `region-{region}.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
                WHERE
                job_id in ({jobs}) and
                Start_Time BETWEEN "{start_time}" AND CURRENT_TIMESTAMP()
                GROUP BY
                1,2,3,4,5,6,7
                ORDER BY
                total_slot_ms DESC;
    """

MP_BUCKET_HISTORIC_QUERY = """
                            CREATE OR REPLACE TABLE {table_name} AS
                            WITH num_products_per_hierarchy AS
                            (
                                SELECT
                                    {hierarchies},
                                    COUNT(distinct product_code) AS num_products
                                FROM {table_name}
                                GROUP BY {hierarchies}
                            ),
                            psuedo_hierarchy_level_per_hierarchy AS
                            (
                                SELECT
                                    *,
                                    CEIL(num_products/{num_products_per_bucket}) AS num_buckets,
                                    {hierarchy_split_concat}
                                FROM num_products_per_hierarchy
                            ),
                            product_validated_with_bucket_num AS
                            (
                                SELECT
                                    T1.*,
                                    T2.psuedo_hierarchy_level,
                                    CAST(CEILING(ROW_NUMBER() OVER (PARTITION BY {window_partition}) * T2.num_buckets * 1.0 / count(*) over (partition by {window_partition})) as int64) as bucket_num
                                FROM {table_name} T1
                                JOIN psuedo_hierarchy_level_per_hierarchy T2
                                ON {join_condition}
                            )
                            SELECT
                                T.* EXCEPT(psuedo_hierarchy_level, bucket_num, mp_bucket_code),
                                CONCAT(psuedo_hierarchy_level, '_', bucket_num) as mp_bucket_code
                            FROM product_validated_with_bucket_num T
                            """

MP_BUCKET_PERIODIC_QUERY = """
                            CREATE OR REPLACE TABLE {table_name} AS
                                WITH new_products_count AS
                                (
                                    SELECT
                                        {hierarchies},
                                        COUNT(distinct product_code) AS products_count
                                    FROM {table_name}
                                    GROUP BY {hierarchies}
                                ),
                                old_products_count AS
                                (
                                    SELECT
                                        {hierarchies},
                                        num_products as products_count,
                                    FROM {bckp_table_name}
                                    GROUP BY {hierarchies}, num_products
                                ),
                                psuedo_hierarchy_level_per_hierarchy AS
                                (
                                    SELECT
                                        {hierarchy_split},
                                        CEIL(T1.products_count/{num_products_per_bucket}) AS num_buckets,
                                        {hierarchy_split_concat},
                                        CASE
                                            WHEN T1.products_count > COALESCE(T2.products_count, 0) THEN true
                                            ELSE false
                                        END AS  regenerate_mp_bucket_code
                                    FROM new_products_count T1
                                    LEFT JOIN old_products_count T2
                                    ON {join_condition}
                                ),
                                product_validated_with_bucket_num AS
                                (
                                    SELECT
                                        T1.*,
                                        T2.psuedo_hierarchy_level,
                                        CAST(CEILING(ROW_NUMBER() OVER (PARTITION BY {window_partition}) * T2.num_buckets * 1.0 / count(*) over (partition by {window_partition})) as int64) as bucket_num,
                                        T2.regenerate_mp_bucket_code
                                    FROM {table_name} T1
                                    JOIN psuedo_hierarchy_level_per_hierarchy T2
                                    ON {join_condition}
                                )
                                SELECT
                                    T1.* EXCEPT(psuedo_hierarchy_level, bucket_num, mp_bucket_code),
                                    CASE
                                        WHEN T1.regenerate_mp_bucket_code=true
                                        THEN CONCAT(psuedo_hierarchy_level, '_', bucket_num)
                                        ELSE T2.mp_bucket_code
                                    END as mp_bucket_code
                                FROM product_validated_with_bucket_num T1
                                LEFT JOIN {bckp_table_name} T2
                                ON T1.product_code = T2.product_code
                           """