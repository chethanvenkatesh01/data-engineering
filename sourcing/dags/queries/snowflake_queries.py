FETCH_PARAMS = """
                        SELECT
                    "table",
                    view,
                    source_config,
                    filter,
                    CAST(TO_TIMESTAMP(filter_param) AS STRING) AS filter_param,
                    replace,
                    connector,
                    schedule_interval,
                    pull_type,
                    source_format_regex,
                    inter_query_exec_order,
                    partition_column,
                    clustering_columns,
                    COALESCE(extraction_sync_dt, TO_TIMESTAMP(DATEADD(day, -1, CURRENT_TIMESTAMP()))) AS extraction_sync_dt,
                    field_delimiter,
                    COALESCE(T.total_rows, 0) AS num_rows,
                    replace_special_characters,
                    row_count_validation_info
                FROM
                    ({from_clause}) M
                LEFT JOIN
                    (
                        SELECT table_schema, table_name, row_count AS total_rows
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE table_schema = '{schema}'
                    ) T
                ON M.view = T.table_name
                ORDER BY
                    inter_query_exec_order;

        """

UPDATE_PARAM_COL_DETAILS = """
                                SELECT 
                                    column_name 
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE table_name='{entity}_intermediate'
                                AND UPPER(column_name)='SYNCSTARTDATETIME'  
            """

FETCH_LATEST_DATE = """
                    select TO_VARCHAR(MAX(TO_TIMESTAMP({param_filter})))
                    from {view_name}
                """