DATA_TYPE_MAPPINGS = {
    "string": {"SNOWFLAKE": "VARCHAR", "BIGQUERY": "STRING"},
    "int": {"SNOWFLAKE": "INTEGER", "BIGQUERY": "INT64"},
    "float": {"SNOWFLAKE": "FLOAT", "BIGQUERY": "FLOAT64"},
    "boolean": {"SNOWFLAKE": "BOOLEAN", "BIGQUERY": "BOOL"},
    "timestamp": {"SNOWFLAKE": "TIMESTAMP", "BIGQUERY": "TIMESTAMP"},
    "date": {"SNOWFLAKE": "DATE", "BIGQUERY": "DATE"},
    "time": {"SNOWFLAKE": "TIME", "BIGQUERY": "TIME"},
    "datetime": {"SNOWFLAKE": "TIMESTAMP_NTZ", "BIGQUERY": "DATETIME"},
    "timestamp_with_zone": {"SNOWFLAKE": "TIMESTAMP_TZ", "BIGQUERY": "TIMESTAMP"},
    "numeric": {"SNOWFLAKE": "NUMERIC", "BIGQUERY": "NUMERIC"},
    "bytes": {"SNOWFLAKE": "BINARY", "BIGQUERY": "BYTES"},
    "json": {"SNOWFLAKE": "VARIANT", "BIGQUERY": "JSON"},
    "array": {"SNOWFLAKE": "ARRAY", "BIGQUERY": "ARRAY"},
    "struct": {"SNOWFLAKE": "OBJECT", "BIGQUERY": "STRUCT"},
    "decimal": {"SNOWFLAKE": "DECIMAL", "BIGQUERY": "DECIMAL"},
    "variant": {"SNOWFLAKE": "VARIANT", "BIGQUERY": "JSON"},
    "geography": {"SNOWFLAKE": "GEOGRAPHY", "BIGQUERY": "GEOGRAPHY"},
    "binary": {"SNOWFLAKE": "BINARY", "BIGQUERY": "BYTES"},
    "double": {"SNOWFLAKE": "DOUBLE", "BIGQUERY": "FLOAT64"},
    "object": {"SNOWFLAKE": "OBJECT", "BIGQUERY": "STRUCT"},
    "text": {"SNOWFLAKE": "TEXT", "BIGQUERY": "STRING"},
    "char": {"SNOWFLAKE": "CHAR", "BIGQUERY": "STRING"},
    "varchar": {"SNOWFLAKE": "VARCHAR", "BIGQUERY": "STRING"}
}

def escape_column_name(column_name: str, warehouse: str) -> str:
    """
    Escape column names for the specified warehouse.
    """
    keywords = [
    "ACCOUNT", "ALL", "ALTER", "AND", "ANY", "AS",
    "BETWEEN", "BY", "CASE", "CAST", "CHECK", "COLUMN",
    "CONNECT", "CONNECTION", "CONSTRAINT", "CREATE", "CROSS",
    "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
    "DATABASE", "DELETE", "DISTINCT", "DROP", "ELSE", "EXISTS",
    "FALSE", "FOLLOWING", "FOR", "FROM", "FULL", "GRANT", "GROUP",
    "GSCLUSTER", "HAVING", "ILIKE", "IN", "INCREMENT", "INNER",
    "INSERT", "INTERSECT", "INTO", "IS", "ISSUE", "JOIN", "LATERAL",
    "LEFT", "LIKE", "LOCALTIME", "LOCALTIMESTAMP", "MINUS", "NATURAL", "NOT",
    "NULL", "OF", "ON", "OR", "ORDER", "ORGANIZATION", "QUALIFY", "REGEXP",
    "REVOKE", "RIGHT", "RLIKE", "ROW", "ROWS", "SAMPLE", "SCHEMA", "SELECT",
    "SET", "SOME", "START", "TABLE", "TABLESAMPLE", "THEN", "TO", "TRIGGER",
    "TRUE", "TRY_CAST", "UNION", "UNIQUE", "UPDATE", "USING", "VALUES", "VIEW",
    "WHEN", "WHENEVER", "WHERE", "WITH"
]

    if warehouse == "SNOWFLAKE":
        return f'"{column_name}"' if column_name.upper() in keywords else column_name
    elif warehouse == "BIGQUERY":
        return f'`{column_name}`'
    return column_name

def generate_create_table_ddl(warehouse: str, table_name: str, column_definitions: dict, partition_by: str = None, clustered_by: str = None) -> str:
    """
    Generate a `CREATE OR REPLACE TABLE` query for the specified warehouse.

    Args:
        warehouse (str): Target warehouse, e.g., 'BIGQUERY' or 'SNOWFLAKE'.
        table_name (str): Name of the table to create.
        column_definitions (dict[str, str]): Dictionary mapping column names to common data types.

    Returns:
        str: SQL query to create or replace the table.
    """
    warehouse = warehouse.upper()
    if warehouse not in ("BIGQUERY", "SNOWFLAKE"):
        raise ValueError("Unsupported warehouse. Supported values are: 'BIGQUERY', 'SNOWFLAKE'.")

    # Prepare column definitions
    columns = []
    for column, data_type in column_definitions.items():
        mapped_type = DATA_TYPE_MAPPINGS.get(data_type, {}).get(warehouse)
        if not mapped_type:
            raise ValueError(f"No mapping found for data type '{data_type}' in warehouse '{warehouse}'.")
        escaped_column = escape_column_name(column, warehouse).lower()
        columns.append(f"{escaped_column} {mapped_type}")

    # Join column definitions and construct query
    columns_str = ",\n  ".join(columns)
    if warehouse == "SNOWFLAKE":
        optimization_clause = ""
        if clustered_by:
            optimization_clause += f" CLUSTER BY ({clustered_by})"
        query = f"CREATE OR REPLACE TABLE {table_name} (\n  {columns_str}\n) {optimization_clause};"
    elif warehouse == "BIGQUERY":
        optimization_clause = ""
        if partition_by:
            optimization_clause += f" PARTITION BY {partition_by}"
        if clustered_by:
            optimization_clause += f" CLUSTER BY {clustered_by}"
        query = f"CREATE OR REPLACE TABLE {table_name} (\n  {columns_str}\n) {optimization_clause};"

    return query

def generate_create_table_ddl_using_select(warehouse: str, table_name: str, query: str, partition_by: str = None, clustered_by: str = None) -> str:
    
    warehouse = warehouse.upper()
    if warehouse not in ("BIGQUERY", "SNOWFLAKE"):
        raise ValueError("Unsupported warehouse. Supported values are: 'BIGQUERY', 'SNOWFLAKE'.")

    if warehouse == "SNOWFLAKE":
        optimization_clause = ""
        if clustered_by:
            optimization_clause += f" CLUSTER BY ({clustered_by})"
        query = f"CREATE OR REPLACE TABLE {table_name} {optimization_clause} (\n  {query}\n);"
    elif warehouse == "BIGQUERY":
        optimization_clause = ""
        if partition_by:
            optimization_clause += f" PARTITION BY {partition_by}"
        if clustered_by:
            optimization_clause += f" CLUSTER BY {clustered_by}"
        query = f"CREATE OR REPLACE TABLE {table_name} {optimization_clause} as (\n  {query}\n) ;"

    return query

def generate_insert_ddl(warehouse: str, table_name: str, column_values: dict) -> str:
    """
    Generate an `INSERT INTO` query for the specified warehouse.

    Args:
        warehouse (str): Target warehouse, e.g., 'BIGQUERY' or 'SNOWFLAKE'.
        table_name (str): Name of the table to insert into.
        column_values (dict[str, any]): Dictionary where keys are column names and values are the values to insert.

    Returns:
        str: SQL query to insert data into the table.
    """
    warehouse = warehouse.upper()
    if warehouse not in ("BIGQUERY", "SNOWFLAKE"):
        raise ValueError("Unsupported warehouse. Supported values are: 'BIGQUERY', 'SNOWFLAKE'.")

    # Prepare column names and values
    columns = ", ".join(escape_column_name(col, warehouse) for col in column_values.keys())
    values = []
    for value in column_values.values():
        if isinstance(value, str) and value.upper().endswith("()"):  
            values.append(value) 
        elif isinstance(value, str):
            values.append(f"'{value}'")
        elif value is None:
            values.append("NULL")
        elif isinstance(value, bool):
            values.append("TRUE" if value else "FALSE")
        else:
            values.append(str(value))

    values_str = ", ".join(values)
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str});"
    return query


def datetime_const(warehouse: str):
    if warehouse == "SNOWFLAKE":
        return  "CURRENT_TIMESTAMP()"
    elif warehouse == "BIGQUERY":
        return  "CURRENT_DATETIME()"

def to_json(warehouse: str):
    if warehouse == "SNOWFLAKE":
        return  "TO_JSON"
    elif warehouse == "BIGQUERY":
        return  "TO_JSON_STRING"

def parse_json(warehouse: str):
    if warehouse == "SNOWFLAKE":
        return  "PARSE_JSON"
    elif warehouse == "BIGQUERY":
        return  "JSON_EXTRACT_ARRAY"

def except_distinct(warehouse: str):
    if warehouse == "SNOWFLAKE":
        return  "except"
    elif warehouse == "BIGQUERY":
        return  "except distinct"

def complex_data_type(warehouse: str):
    if warehouse == "SNOWFLAKE":
        return  ["VARIANT","ARRAY","OBJECT"]
    elif warehouse == "BIGQUERY":
        return  ["JSON","ARRAY","STRUCT"]
