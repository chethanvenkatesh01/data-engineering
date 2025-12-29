FETCH_PARAMS = """
                    SELECT
                        table,
                        view,
                        source_config,
                        filter,
                        CAST(DATETIME(filter_param) AS string) AS filter_param,
                        replace,
                        connector,
                        schedule_interval,
                        pull_type,
                        source_format_regex,
                        inter_query_exec_order,
                        partition_column,
                        clustering_columns,
                        coalesce(extraction_sync_dt,datetime_sub(current_datetime(), interval 1 day)) as extraction_sync_dt,
                        field_delimiter,
                        coalesce(T.total_rows,0) AS num_rows,
                        replace_special_characters,
                        row_count_validation_info,
                        miscellaneous_attributes
                    FROM
                    (
                        {from_clause}
                    ) M
                    LEFT JOIN
                      (
                        SELECT table_schema, table_name, total_rows 
                        FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.TABLE_STORAGE
                        WHERE table_schema='{gcp_dataset}'
                    ) T
                    ON REGEXP_REPLACE(M.view, r'[^A-Za-z0-9_]', '_') = T.table_name
                    ORDER BY
                        inter_query_exec_order
                """


UPDATE_PARAM_COL_DETAILS = """
                                SELECT 
                                    column_name 
                                FROM {project}.{dataset}.INFORMATION_SCHEMA.COLUMNS
                                WHERE table_name='{entity}_intermediate'
                                AND UPPER(column_name)='SYNCSTARTDATETIME'
                    """

FETCH_LATEST_DATE = """
                    select cast(max(cast({param_filter} as datetime)) as string) 
                    from {view_name}
                """
