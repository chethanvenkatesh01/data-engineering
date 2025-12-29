TRANSFORM_VALIDATE = """
-- Create the base validation table from the intermediate table with required checks
CREATE OR REPLACE TABLE {validated_table_name} AS
WITH 
    transformed_table AS (
        SELECT * EXCLUDE ({except_cols_const}),
               {transformed_cols_final}
        FROM (
            SELECT *, NULL AS dummy, NULL AS handle_except
            FROM {raw_table_name}
            {where_filter_clause}
        )
    ),
    name_type_update AS (
        SELECT * EXCLUDE ({old_cols}),
               {typecast_cols_const}
        FROM transformed_table
    ),
    null_check AS (
        SELECT *,
               CASE WHEN ({null_cols_const}) THEN TRUE ELSE FALSE END AS null_check
        FROM name_type_update
    ),
    primary_key_check AS (
        SELECT * EXCLUDE (primary_duplicate_rank, handle_except),
               CASE WHEN primary_duplicate_rank > 1 THEN TRUE ELSE FALSE END AS primary_key_check
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY {level_cols_const} order by {level_cols_const}) AS primary_duplicate_rank
            FROM null_check
        )
    )
SELECT *
FROM primary_key_check;

-- Create the anomaly summary table with row counts
CREATE OR REPLACE TABLE {anomaly_summary_table_name} AS
(
    SELECT CAST('{entity_type}' AS VARCHAR(16777216)) AS "table",
           CAST('validation' AS VARCHAR(16777216)) AS module,
           CAST('null_check' AS VARCHAR(16777216)) AS rule,
           CAST('null_check' AS VARCHAR(16777216)) AS rule_display_name,
           CAST('' AS VARCHAR(16777216)) AS rule_description,
           CAST('DELETE' AS VARCHAR(16777216)) AS action,
           COUNT(*) AS no_original_rows,
           COUNT_IF(null_check) AS affected_rows,
           COUNT_IF(null_check)::FLOAT / COUNT(*) AS affected_rows_percent,
           CAST(CASE WHEN COUNT_IF(null_check) > 0 THEN 'Fail' ELSE 'Pass' END AS VARCHAR(16777216)) AS status,
           CURRENT_TIMESTAMP() AS insertion_date
    FROM {validated_table_name}
)
UNION ALL
(
    SELECT CAST('{entity_type}' AS VARCHAR(16777216)) AS "table",
           CAST('validation' AS VARCHAR(16777216)) AS module,
           CAST('primary_key_check' AS VARCHAR(16777216)) AS rule,
           CAST('primary_key_check' AS VARCHAR(16777216)) AS rule_display_name,
           CAST('' AS VARCHAR(16777216)) AS rule_description,
           CAST('DELETE' AS VARCHAR(16777216)) AS action,
           COUNT(*) AS no_original_rows,
           COUNT_IF(primary_key_check) AS affected_rows,
           COUNT_IF(primary_key_check)::FLOAT / COUNT(*) AS affected_rows_percent,
           CAST(CASE WHEN COUNT_IF(primary_key_check) > 0 THEN 'Fail' ELSE 'Pass' END AS VARCHAR(16777216)) AS status,
           CURRENT_TIMESTAMP() AS insertion_date
    FROM {validated_table_name}
);
-- Update the rule_description
UPDATE {anomaly_summary_table_name}
SET rule_description = CASE
        WHEN rule = 'null_check' THEN (
            SELECT ARRAY_TO_STRING(
                ARRAY_AGG(CONCAT(col_name, ' - ', nulls_count)), ', '
            )
            FROM (
                {null_query}
            )
        )
        WHEN rule = 'primary_key_check' THEN '{level_cols_const}'
        ELSE rule_description
    END,
    status = CASE WHEN affected_rows > 0 THEN 'Fail' ELSE 'Pass' END;

-- Create the anomaly table and copy defective rows
CREATE OR REPLACE TABLE {anomaly_table_name} AS
SELECT * EXCLUDE (null_check, primary_key_check),
       CASE WHEN null_check = TRUE THEN 'null_check'
            WHEN primary_key_check = TRUE THEN 'primary_key_check'
            ELSE NULL END AS QC,
       'DELETE' AS action,
       CURRENT_TIMESTAMP() AS insertion_date
FROM {validated_table_name}
WHERE null_check = TRUE OR primary_key_check = TRUE;

-- Delete defective rows
DELETE FROM {validated_table_name}
WHERE null_check = TRUE OR primary_key_check = TRUE;

-- Drop null_check and primary_key_check columns
ALTER TABLE {validated_table_name} DROP COLUMN IF EXISTS null_check;
ALTER TABLE {validated_table_name} DROP COLUMN IF EXISTS primary_key_check;
"""


ANOMALY_METRIC_QUERY = """
WITH main AS (
    SELECT * 
    FROM (
        SELECT 
            'value' AS type,
            CAST(initial_rc AS STRING) AS initial_data,
            CAST((initial_rc - after_nullcheck_rc) AS STRING) AS null_data_loss,
            CAST((after_nullcheck_rc - duplicate_level_check_rc) AS STRING) AS duplicate_level_data_loss,
            CAST((initial_rc - duplicate_level_check_rc) AS STRING) AS total_loss
        FROM {validated_table_name}
        GROUP BY 1, 2, 3, 4, 5

        UNION ALL

        SELECT 
            'percentage' AS type,
            CAST(initial_rc AS STRING) AS initial_data,
            CAST((CASE WHEN initial_rc != 0 THEN (initial_rc - after_nullcheck_rc) * 100.0 / initial_rc ELSE 0 END) AS STRING) AS null_data_loss,
            CAST((CASE WHEN initial_rc != 0 THEN (after_nullcheck_rc - duplicate_level_check_rc) * 100.0 / initial_rc ELSE 0 END) AS STRING) AS duplicate_level_data_loss,
            CAST((CASE WHEN initial_rc != 0 THEN ABS(initial_rc - duplicate_level_check_rc) * 100.0 / initial_rc ELSE 0 END) AS STRING) AS total_loss
        FROM {validated_table_name}
    )
    GROUP BY 1, 2, 3, 4, 5

    UNION ALL

    SELECT 
        type, 
        initial_data, 
        null_data_loss, 
        duplicate_level_data_loss, 
        total_loss
    FROM (
        SELECT 
            'error_rc' AS type,
            '' AS initial_data,
            CONCAT('[ ', LISTAGG(CONCAT(' ', col_name, ' - ', nulls_count), ', ') WITHIN GROUP (ORDER BY col_name), ' ]') AS null_data_loss,
            '[{level_cols_const}"]' AS duplicate_level_data_loss,
            '' AS total_loss
        FROM (
            SELECT 
                col_name, 
                COUNT(1) AS nulls_count
            FROM (
                SELECT * 
                FROM {raw_table_name}
                {where_filter_clause}
            ) t,
            TABLE(FLATTEN(INPUT => PARSE_JSON(TO_VARIANT(t)))) col_name
            WHERE LOWER(col_name.VALUE) IN ('{null_col_const}')
            GROUP BY col_name
        )
    )
    GROUP BY 1, 2, 3, 4, 5
),
defaults AS (
    SELECT 'value' AS type, '0' AS initial_data, '0' AS null_data_loss, '0' AS duplicate_level_data_loss, '0' AS total_loss
    UNION ALL
    SELECT 'percentage' AS type, '0' AS initial_data, '0' AS null_data_loss, '0' AS duplicate_level_data_loss, '0' AS total_loss
    UNION ALL
    SELECT 'error_rc' AS type, '' AS initial_data, '' AS null_data_loss, '' AS duplicate_level_data_loss, '' AS total_loss
)
SELECT * FROM main
UNION ALL
SELECT * FROM defaults
WHERE NOT EXISTS (SELECT * FROM main);
"""

MERGE_QUERY = """
            MERGE INTO {destination_table} AS target
            USING ({master_table_query}) AS source
            ON {on_condition_const} 
            WHEN NOT MATCHED THEN
                INSERT ({master_cols_const}) 
                VALUES ({source_master_cols_const})
            WHEN MATCHED THEN
                UPDATE SET {update_set_const};
            """
UPDATE_MERGE_QUERY ="""
            UPDATE {destination_table}
            SET active = false
            WHERE {level_const} NOT IN (
                SELECT {level_const} FROM {source_table} );
"""

DELETE_MERGE_QUERY ="""
            DELETE FROM {destination_table}
            WHERE {level_const} NOT IN (
                SELECT {level_const} FROM {source_table}) ;
"""

COPY_ANOMALY_DATA_TO_SNOWFLAKE = """
DECLARE
    column_list STRING;
    copy_stmt STRING;
BEGIN
    -- Generate the column list and store it in a variable
    SELECT LISTAGG(CASE
        WHEN data_type != 'ARRAY' THEN '' || column_name || '' 
        ELSE 'ARRAY_TO_STRING(' || '"' || column_name || '"' || ', '' | '') AS ' || column_name || ''
        END, ', ') 
    WITHIN GROUP (ORDER BY ordinal_position)
    INTO column_list
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = upper('{schema}') 
      AND table_name = upper('{et}_anomaly')
      AND column_name NOT IN ('initial_rc', 'after_nullcheck_rc', 'primary_duplicate_rank', 'duplicate_level_check_rc', 'insertion_date');

    -- Construct the COPY INTO statement dynamically using the column list
    copy_stmt := 'COPY INTO ''{gcs_uri}'' ' ||
                 'FROM ( ' ||
                 'SELECT ' || column_list || ' ' ||
                 'FROM {schema}.{et}_anomaly ' ||
                 ') ' ||
                 'FILE_FORMAT = ( ' ||
                 'TYPE = ''CSV'', ' ||
                 'FIELD_OPTIONALLY_ENCLOSED_BY = ''"'', ' ||
                 'FIELD_DELIMITER = '','' ' ||
                 'COMPRESSION = ''NONE'' ' ||
                 ') ' ||
                 'OVERWRITE = TRUE ' ||
                 'STORAGE_INTEGRATION = "{storage_integration}";';

    -- Execute the dynamic COPY INTO statement
    EXECUTE IMMEDIATE copy_stmt;
END;
"""


MASTER_TABLE_TRANSCATION = """
                    BEGIN
                        delete from {master_table} where date > current_date()-{backfill} ;
                        insert into {master_table} ({master_cols_const})
                        select {master_cols_const})
                        from
                        (
                            select * from {master_validated_table}
                        );
                        COMMIT;
                        EXCEPTION
                        WHEN other THEN
                            -- Log the error message
                            RAISE; 
                        END;
                    """

LSI_QUERY_MV = """
        CREATE OR REPLACE MATERIALIZED VIEW {master_table_imputed}
        AS (
        select * from {master_table}
        );
    """

SLOTS_QUERY = """
        WITH
        warehouse_sizes AS (
            SELECT 'X-Small' AS warehouse_size, 1 AS credits_per_hour UNION ALL
            SELECT 'Small' AS warehouse_size, 2 AS credits_per_hour UNION ALL
            SELECT 'Medium'  AS warehouse_size, 4 AS credits_per_hour UNION ALL
            SELECT 'Large' AS warehouse_size, 8 AS credits_per_hour UNION ALL
            SELECT 'X-Large' AS warehouse_size, 16 AS credits_per_hour UNION ALL
            SELECT '2X-Large' AS warehouse_size, 32 AS credits_per_hour UNION ALL
            SELECT '3X-Large' AS warehouse_size, 64 AS credits_per_hour UNION ALL
            SELECT '4X-Large' AS warehouse_size, 128 AS credits_per_hour
        )
            SELECT
                        current_timestamp() as date,
                        query_id as job_id,
                        user_name as user_email,
                        database_id as project_id,
                        TO_CHAR(CONVERT_TIMEZONE('UTC', start_time), 'YYYY-MM-DD HH24:MI:SS.FF') as job_start_time,
                        TO_CHAR(CONVERT_TIMEZONE('UTC', end_time), 'YYYY-MM-DD HH24:MI:SS.FF') as job_end_time,
                        execution_time as time_taken_inseconds,
                        null AS total_slot_ms,
                        bytes_scanned AS total_bytes_processed,
                        concat(cast(qh.execution_time/(1000*60*60)*wh.credits_per_hour as string), '$') AS total_slot_cost
        FROM snowflake.account_usage.query_history AS qh
        INNER JOIN warehouse_sizes AS wh
            ON qh.warehouse_size=wh.warehouse_size
                        WHERE
                        query_id in ({jobs}) and
                        start_time BETWEEN "{start_time}" AND CURRENT_TIMESTAMP()
                    
                        ORDER BY
                        total_slot_cost DESC;
    """

MP_BUCKET_HISTORIC_QUERY = """
                            CREATE OR REPLACE TABLE {table_name} AS
                            WITH num_products_per_hierarchy AS (
                            SELECT
                                {hierarchies},
                                COUNT(DISTINCT product_code) AS num_products
                            FROM {table_name}
                            GROUP BY {hierarchies}
                            ),
                            psuedo_hierarchy_level_per_hierarchy AS (
                            SELECT
                                *,
                                CEIL(num_products/{num_products_per_bucket}) AS num_buckets,
                                {hierarchy_split_concat}
                            FROM num_products_per_hierarchy
                            ),
                            product_validated_with_bucket_num AS (
                            SELECT
                                T1.*,
                                T2.psuedo_hierarchy_level,
                                CAST(CEILING(ROW_NUMBER() OVER (
                                    PARTITION BY {window_partition}
                                    ORDER BY T1.product_code
                                ) * T2.num_buckets * 1.0 / COUNT(*) OVER (PARTITION BY {window_partition})) AS INTEGER) as bucket_num
                            FROM {table_name} T1
                            JOIN psuedo_hierarchy_level_per_hierarchy T2
                            ON {join_condition}
                            )
                            SELECT
                            T.* EXCLUDE(psuedo_hierarchy_level, bucket_num, mp_bucket_code),
                            CONCAT(psuedo_hierarchy_level, '_', bucket_num) as mp_bucket_code
                            FROM product_validated_with_bucket_num T;
                            """

MP_BUCKET_PERIODIC_QUERY = """
                            CREATE OR REPLACE TABLE {table_name} AS
                            WITH new_products_count AS (
                            SELECT 
                                {hierarchies},
                                COUNT(DISTINCT product_code) AS products_count
                            FROM {table_name}
                            GROUP BY l0_name, l1_name, l2_name
                            ),
                            old_products_count AS (
                            SELECT
                                {hierarchies},
                                num_products as products_count
                            FROM {bckp_table_name}
                            GROUP BY {hierarchies}, num_products
                            ),
                            psuedo_hierarchy_level_per_hierarchy AS (
                            SELECT
                                {hierarchy_split},
                                CEIL(T1.products_count/200) AS num_buckets,
                                {hierarchy_split_concat},
                                CASE
                                    WHEN T1.products_count > COALESCE(T2.products_count, 0) THEN TRUE
                                    ELSE FALSE
                                END AS regenerate_mp_bucket_code
                            FROM new_products_count T1
                            LEFT JOIN old_products_count T2
                                ON {join_condition}
                            ),
                            product_validated_with_bucket_num AS (
                            SELECT
                                T1.*,
                                T2.psuedo_hierarchy_level,
                                CAST(CEIL(ROW_NUMBER() OVER (
                                PARTITION BY {window_partition}
                                ORDER BY T1.product_code
                            ) * T2.num_buckets * 1.0 / COUNT(*) OVER (PARTITION BY {window_partition})) AS INTEGER) as bucket_num,
                                T2.regenerate_mp_bucket_code
                            FROM {table_name} T1
                            JOIN psuedo_hierarchy_level_per_hierarchy T2
                                ON {join_condition}
                            )
                            SELECT 
                            T1.* EXCLUDE(psuedo_hierarchy_level, bucket_num, mp_bucket_code),
                            CASE
                                WHEN T1.regenerate_mp_bucket_code=TRUE 
                                THEN CONCAT(psuedo_hierarchy_level, '_', bucket_num)
                                ELSE T2.mp_bucket_code
                            END as mp_bucket_code
                            FROM product_validated_with_bucket_num T1
                            LEFT JOIN {bckp_table_name} T2
                            ON T1.product_code = T2.product_code;
                           """