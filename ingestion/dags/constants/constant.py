from enum import Enum


def get_tenant_secret():

    import os
    from dotenv import load_dotenv
    from google.cloud import secretmanager

    load_dotenv()

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{os.getenv('GCP_PROJECT')}/secrets/{os.getenv('tenant')}_{os.getenv('pipeline')}_data_ingestion/versions/latest"
    response = client.access_secret_version(name=name)
    tenant_config: dict = eval(response.payload.data.decode("UTF-8"))
    return tenant_config


def set_pg_vars():
    from airflow import models

    tenant_config = get_tenant_secret()
    models.Variable.set("PGXUSER", tenant_config["pg_user"])
    models.Variable.set("PGXPASSWORD", tenant_config["pg_password"])
    models.Variable.set("PGXHOST", tenant_config["pg_host"])
    models.Variable.set("PGXDATABASE", tenant_config["pg_database"])
    models.Variable.set("PGXPORT", tenant_config["pg_port"])
    models.Variable.set("PGXSCHEMA", tenant_config["pg_schema"])


def set_tenant_alias():
    import os
    from airflow import models

    tenant_config = get_tenant_secret()
    tenant_alias = tenant_config.get("tenant-alias") or os.getenv("tenant")
    models.Variable.set("tenant_alias", tenant_alias)


def database_to_warehouse_dtypes(warehouse):
    if warehouse == "SNOWFLAKE":
        return {
            "tinyint": "NUMBER",
            "smallint": "SMALLINT",
            "mediumint": "NUMBER",
            "int": "INTEGER",
            "int4": "INTEGER",
            "int8": "BIGINT",
            "integer": "INTEGER",
            "bigint": "BIGINT",
            "serial": "INTEGER",
            "bigserial": "BIGINT",
            "float": "FLOAT",
            "float8": "FLOAT",
            "real": "FLOAT",
            "double precision": "DOUBLE",
            "decimal": "NUMBER",
            "numeric": "NUMBER",
            "boolean": "BOOLEAN",
            "bool": "BOOLEAN",
            "char": "CHAR",
            "character": "CHAR",
            "varchar": "VARCHAR",
            "character varying": "VARCHAR",
            "text": "TEXT",
            "bytea": "BINARY",
            "json": "VARIANT",
            "jsonb": "VARIANT",
            "xml": "TEXT",
            "uuid": "TEXT",
            "date": "DATE",
            "time": "TIME",
            "time without time zone": "TIME",
            "time with time zone": "TIMESTAMP_TZ",
            "timestamp": "TIMESTAMP",
            "timestamp without time zone": "TIMESTAMP",
            "timestamp with time zone": "TIMESTAMP_TZ",
            "interval": "INTERVAL",
            # Array Types (Snowflake uses VARIANT for arrays)
            "tinyint[]": "ARRAY",
            "int[]": "ARRAY",
            "float[]": "ARRAY",
            "smallint[]": "ARRAY",
            "mediumint[]": "ARRAY",
            "int8[]": "ARRAY",
            "int4[]": "ARRAY",
            "integer[]": "ARRAY",
            "bigint[]": "ARRAY",
            "decimal[]": "ARRAY",
            "numeric[]": "ARRAY",
            "real[]": "ARRAY",
            "double[]": "ARRAY",
            "precision[]": "ARRAY",
            "float8[]": "ARRAY",
            "boolean[]": "ARRAY",
            "char[]": "ARRAY",
            "character[]": "ARRAY",
            "varchar[]": "ARRAY",
            "bytes[]": "ARRAY",
            "tinytext[]": "ARRAY",
            "text[]": "ARRAY",
            "mediumtext[]": "ARRAY",
            "longtext[]": "ARRAY",
            "date[]": "ARRAY",
            "time[]": "ARRAY",
            "timestamp[]": "ARRAY",
            "bool[]": "ARRAY",
            # Special PostgreSQL types
            "geometry": "GEOGRAPHY",
            "geography": "GEOGRAPHY",
        }

    elif warehouse == "BIGQUERY":
        return {
            "tinyint": "int64",
            "int": "int64",
            "float": "float64",
            "smallint": "int64",
            "mediumint": "int64",
            "int8": "int64",
            "int4": "int64",
            "integer": "int64",
            "bigint": "int64",
            "decimal": "numeric",
            "numeric": "numeric",
            "real": "float64",
            "double": "float64",
            "precision": "float64",
            "float8": "float64",
            "boolean": "bool",
            "char": "string",
            "character": "string",
            "varchar": "string",
            "bytes": "bytes",
            "tinytext": "string",
            "text": "string",
            "mediumtext": "string",
            "longtext": "string",
            "date": "date",
            "time": "time",
            "timestamp": "timestamp",
            "datetime": "datetime",
            "timestamp with time zone": "timestamp",
            "bool": "boolean",
            # Array Types
            "tinyint[]": "ARRAY<int64>",
            "int[]": "ARRAY<int64>",
            "float[]": "ARRAY<float64>",
            "smallint[]": "ARRAY<int64>",
            "mediumint[]": "ARRAY<int64>",
            "int8[]": "ARRAY<int64>",
            "int4[]": "ARRAY<int64>",
            "integer[]": "ARRAY<int64>",
            "bigint[]": "ARRAY<int64>",
            "decimal[]": "ARRAY<numeric>",
            "numeric[]": "ARRAY<numeric>",
            "real[]": "ARRAY<float64>",
            "double[]": "ARRAY<float64>",
            "precision[]": "ARRAY<float64>",
            "float8[]": "ARRAY<float64>",
            "boolean[]": "ARRAY<bool>",
            "char[]": "ARRAY<string>",
            "character[]": "ARRAY<string>",
            "varchar[]": "ARRAY<string>",
            "bytes[]": "ARRAY<bytes>",
            "tinytext[]": "ARRAY<string>",
            "text[]": "ARRAY<string>",
            "mediumtext[]": "ARRAY<string>",
            "longtext[]": "ARRAY<string>",
            "date[]": "ARRAY<date>",
            "time[]": "ARRAY<time>",
            "timestamp[]": "ARRAY<timestamp>",
            "bool[]": "ARRAY<boolean>",
        }
    else:
        raise ValueError("Unsupported warehouse!")


def generate_cast_string(warehouse, data_type, p1, p2, p3):
    """
    paramters:
    warehouse: e.g. BIGQUERY
    data_type: data type
    p1,p2,p3: safe_cast(p1 as p2) as p3
    """
    if warehouse == "BIGQUERY":
        return f"SAFE_CAST({p1} as {p2}) as {p3}"
    elif warehouse == "SNOWFLAKE":
        if data_type.upper() != "ARRAY":
            return f"TRY_CAST(CAST(({p1}) as STRING) as {p2}) as {p3}"
        else:
            return f"CAST({p1} as {p2}) as {p3}"
        # if data_type.upper() not in  ['STRING', 'CHAR', 'VARCHAR']:
        #     return f"case when ({p1}) is null then null else CAST(({p1}) as {p2}) end as {p3}"
        # else:
        #     return f"TRY_CAST({p1} as {p2}) as {p3}"
    else:
        raise ValueError("Unsupported warehouse!")


OWNER = "impact"


class NotificationType(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    QC = "QC"


DB_GBQ_COPY_TABLES_JAR = "gs://dataflow-uber-jars/db-to-bq-pipeline-1.7.0.jar"
GBQ_DB_COPY_TABLES_JAR = "gs://dataflow-uber-jars/bq-to-db-pipeline-1.4.3.jar"
GBQ_DB_COPY_MULTI_TABLES_JAR = "gs://dataflow-uber-jars/bq-to-db-copy-multi-pipeline-1.4.3.jar"
DB_SNOWFLAKE_TABLES_JAR = "gs://dataflow-uber-jars/db-to-sf-pipeline-1.1.0.jar"
SNOWFLAKE_DB_COPY_TABLES_JAR = (
    "gs://dataflow-uber-jars/sf-to-db-pipeline-1.1.0.jar"
)
SNOWFLAKE_DB_COPY_MULTI_TABLES_JAR = (
    "gs://dataflow-uber-jars/sf-to-db-copy-multi-pipeline-1.1.0.jar"
)



DERIVED_TABLES_MAPPING_TABLE = "derived_tables_mapping"

ENV_SCHEMA = "data_platform"

ALLOWED_HTTP_REQUEST_TYPES = {"GET", "PUT", "POST"}

ALLOWED_HTTP_RESPONSE_CODES = {200, 201, 202, 203, 204}


def get_overall_config():
    from airflow.models import Variable

    overall_config = Variable.get(
        "overall_config",
        default_var={
            "ingestion_config": {},
            "entities_config": {
                "destination_table": ["dummy"],
                "pull_type": ["dummy"],
                "data_loss_threshold": ["dummy"],
                "pg_persistence": ["dummy"],
                "raw_table": ["dummy"],
                "mapping_table": ["dummy"],
                "attribute_table_list": ["dummy"],
                "attribute_hierarchy_list": ["dummy"],
                "entity_type": ["dummy"],
            },
            "der_tab_config": {
                "der_tabs_names_list": [],
                "der_tabs_exec_order_dict": {},
                "der_tabs_info_dict": {},
                "der_tabs_copy_to_psg_job_position": "",
            },
        },
        deserialize_json=True,
    )

    return overall_config
