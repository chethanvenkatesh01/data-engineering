import re
import os
import time
import json
from datetime import datetime
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook

from dataflow_options.utils import Logger
from airflow_options.utils import TryExcept, get_source_config_value

from google.cloud import bigquery
from google.cloud import storage

import snowflake.connector
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import ResultMetadata

import gcsfs
import pyarrow.parquet as pq
import zlib

log = Logger(__name__)

class SnowflakeToGcsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, gcs_destination:str,
                 snowflake_conn_id:str='snowflake_default', 
                 config_secret_name:str=None, 
                 warehouse:str=None,
                 role:str=None,  
                 database:str=None, 
                 schema:str=None,
                 table:str=None,
                 sql:str=None,
                 file_format:str='parquet',
                 overwrite:bool=True,
                 pull_type:str='full',
                 incremental_column:str = None,
                 incremental_column_value:str = None,
                 **kwargs) -> None:
        self.gcs_destination = gcs_destination.strip('/')
        self.snowflake_conn_id = snowflake_conn_id
        self.config_secret_name = config_secret_name
        self.warehouse = warehouse
        self.role = role
        self.database = database
        self.schema = schema
        self.table = table
        self.sql = sql
        self.file_format = file_format
        self.overwrite = overwrite
        self.pull_type = pull_type
        self.incremental_column = incremental_column
        self.incremental_column_value = incremental_column_value

    @TryExcept
    def execute(self, context):
        if self.config_secret_name:
            # Get the connection information from secret manager
            config = get_source_config_value(self.config_secret_name)
            username = config.get('username', None) or config.get('user', None)
            password = config.get('password', None)
            account = config.get('account', None)
            warehouse = config.get('warehouse', None)
            database = config.get('database', None)
            schema = config.get('schema', None)
            storage_integration = config.get('storage_integration', None)
            user_role = config.get('role', "ACCOUNTADMIN")
        elif self.snowflake_conn_id:
            # Get the connection information from the airflow connections
            # TODO : This is not required now but can be implemented later 
            #       if a single snowflake instance is used in entire environment
            # Get a 'Connection' object from airflow connections
            # airflow_conn = BaseHook.get_connection(self.snowflake_conn_id)
            pass

        elif os.getenv('AIRFLOW_CONN_SNOWFLAKE_DEFAULT'):
            # Create a connection instance if the AIRFLOW_CONN_SNOWFLAKE_DEFAULT is set in the env variables
            # TODO : This is not required now but can be implemented later 
            #       if a single snowflake instance is used in entire environment
            pass

        assert username is not None, 'snowflake username is not provided'
        assert password is not None, 'snowflake password is not provided'
        assert account is not None, 'snowflake account is not provided'
        assert warehouse is not None, 'snowflake warehouse is not provided'
        assert self.table is not None or self.sql is not None, 'Either table or sql argument should be set'
        assert storage_integration is not None, 'Storage Integration is not provided'

        snowflake_conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=user_role
        )

        cur = snowflake_conn.cursor()

        if self.table:
            assert self.database is not None, 'database is not specified'
            assert self.schema is not None, 'schema is not specified'
            table_expression = f"SELECT * FROM {database}.{schema}.{self.table}"
        elif self.sql:
            table_expression = self.sql
        else:
            raise Exception(f"Either table or sql arugment should be provided")
        
        if self.pull_type.upper()=='INCREMENTAL':
            table_expression = table_expression + f" WHERE {self.incremental_column} > '{self.incremental_column_value}'"

        query = f"""
        COPY INTO '{self.gcs_destination}'
        FROM {'('+ table_expression +')'}
        STORAGE_INTEGRATION = {storage_integration}
        FILE_FORMAT = {self._get_file_format_expression()}
        OVERWRITE={self._get_overwrite_expression()}
        """
        log.info(f"Executing the data unloading query:\n {query}")

        cur.execute_async(query)

        query_id = cur.sfqid
        log.info(f"QueryId: {query_id}")

        # The get_query_status_throw_if_error will automatically raise the "ProgrammingError" if the query failed
        while snowflake_conn.is_still_running(snowflake_conn.get_query_status_throw_if_error(query_id)):
            print("The query is still running. Sleeping for 15 seconds")
            time.sleep(15)
        
        log.info(f"The table has been unloaded successfully to {self.gcs_destination}")

        # if snowflake_conn.get_query_status(query_id).name == 'SUCCESS':
        #     log.info(f"The table has been unloaded successfully to {self.gcs_destination}")
        # elif snowflake_conn.get_query_status(query_id).name in ['FAILED','FAILED_WITH_ERROR']:
        #     log.error(f"The data unloading from table has failed")
    
    def _get_file_format_expression(self):
        if self.file_format.upper() == 'PARQUET':
            return '(TYPE=PARQUET COMPRESSION=SNAPPY)'
        elif self.file_format.upper() == 'CSV':
            return '(TYPE=CSV COMPRESSION=AUTO)'
        else:
            raise Exception(f"{self.file_format} file format is not supported.")
    
    def _get_overwrite_expression(self):
        if self.overwrite:
            return "TRUE"
        else:
            return "FALSE"
    
class SnowflakeToBigQueryOperator(BaseOperator):
    @apply_defaults
    def __init__(self, 
                 bq_dataset:str,
                 bq_table:str,
                 staging_gcs_location:str,
                 bq_project:str = None,
                 snowflake_conn_id:str='snowflake_default', 
                 config_secret_name:str=None, 
                 warehouse:str=None,
                 role:str=None,  
                 database:str=None, 
                 schema:str=None,
                 table:str=None,
                 sql:str=None,
                 staging_file_prefix:str=None,
                 staging_file_format:str='parquet',
                 staging_file_delimiter:str = '|',
                 overwrite_files:bool=True,
                 replace_destination_table:bool=False,
                 pull_type:str='full',
                 incremental_column:str = None,
                 incremental_column_value:str = None,
                 bq_partition_column:str = None,
                 bq_clustering_columns:List[str]=None,
                 audit_column_name:str = "SYNCSTARTDATETIME",
                 bigquery_region: str = "US",
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
        self.staging_gcs_location = staging_gcs_location.strip('/')
        self.snowflake_conn_id = snowflake_conn_id
        self.config_secret_name = config_secret_name
        self.warehouse = warehouse
        self.role = role
        self.database = database
        self.schema = schema
        self.table = table
        self.sql = sql
        self.staging_file_prefix = staging_file_prefix
        self.staging_file_format = staging_file_format
        self.staging_file_delimiter = staging_file_delimiter
        self.overwrite_files = overwrite_files
        self.replace_destination_table = replace_destination_table
        self.pull_type = pull_type
        self.incremental_column = incremental_column
        self.incremental_column_value = incremental_column_value
        self.bq_partition_column = bq_partition_column
        self.bq_clustering_columns = bq_clustering_columns
        self.audit_column_name = audit_column_name
        self.bigquery_region = bigquery_region

    @TryExcept
    def execute(self, context):
        from airflow.models import Variable
        self.bq_client = bigquery.Client(location=self.bigquery_region)
        if self.config_secret_name:
            # Get the connection information from secret manager
            config = get_source_config_value(self.config_secret_name)
            log.info(config)
            username = config.get('username', None) or config.get('user', None)
            password = config.get('password', None)
            account = config.get('account', None)
            warehouse = config.get('warehouse', None)
            database = config.get('database', None)
            schema = config.get('schema', None)
            storage_integration = config.get('storage_integration', None)
            user_role = config.get('role', "ACCOUNTADMIN") 
        elif self.snowflake_conn_id:
            # Get the connection information from the airflow connections
            # TODO : This is not required now but can be implemented later 
            #       if a single snowflake instance is used in entire environment
            # Get a 'Connection' object from airflow connections
            # airflow_conn = BaseHook.get_connection(self.snowflake_conn_id)
            pass

        elif os.getenv('AIRFLOW_CONN_SNOWFLAKE_DEFAULT'):
            # Create a connection instance if the AIRFLOW_CONN_SNOWFLAKE_DEFAULT is set in the env variables
            # TODO : This is not required now but can be implemented later 
            #       if a single snowflake instance is used in entire environment
            pass

        self.database = database
        self.schema = schema

        assert username is not None, 'snowflake username is not provided'
        assert password is not None, 'snowflake password is not provided'
        assert account is not None, 'snowflake account is not provided'
        assert warehouse is not None, 'snowflake warehouse is not provided'
        assert self.table is not None or self.sql is not None, 'Either table or sql argument should be set'
        assert storage_integration is not None, 'Storage Integration is not provided'

        snowflake_conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=user_role
        )

        cur = snowflake_conn.cursor()

        if self.table:
            assert self.database is not None, 'database is not specified'
            assert self.schema is not None, 'schema is not specified'
            table_expression = f"SELECT * FROM {database}.{schema}.{self.table}"
        elif self.sql:
            table_expression = self.sql
        else:
            raise ValueError("Either table or sql arugment should be provided")
        
        if self.pull_type.upper()=='INCREMENTAL':
            table_expression = table_expression + f" WHERE CAST({self.incremental_column} AS DATETIME) > '{self.incremental_column_value}'"
        
        # Get the query result metadata
        log.info("Fetching the query result metadata")
        query_result_metadata = cur.describe(table_expression)

        audit_column_exists = self._check_audit_column(query_result_metadata, self.audit_column_name)
        if not audit_column_exists:
            log.info(f"Adding audit column {self.audit_column_name} to the query")
            table_expression = f"SELECT *, SYSDATE() AS {self.audit_column_name} FROM ({table_expression})"
        else:
            log.info(f"Audit column {self.audit_column_name} already exists as part of the table")
        
        log.info(f"Final Query: {table_expression}")
        
        if self.staging_file_prefix:
            gcs_destination = f"{self.staging_gcs_location}/{self.staging_file_prefix}"
        else:
            gcs_destination = f"{self.staging_gcs_location}/{self.staging_gcs_location.split('/')[-1]}"
        
        log.info(f"GCS Staging Location : {gcs_destination}")

        query = f"""
        COPY INTO '{gcs_destination.replace('gs://','gcs://')}'
        FROM {'('+ table_expression +')'}
        STORAGE_INTEGRATION = {storage_integration}
        FILE_FORMAT = {self._get_file_format_expression()}
        HEADER = TRUE
        OVERWRITE={self._get_overwrite_expression()}
        """
        log.info(f"Executing the data unloading query:\n {query}")

        cur.execute_async(query)

        query_id = cur.sfqid
        log.info(f"QueryId: {query_id}")

        # The get_query_status_throw_if_error will automatically raise the "ProgrammingError" if the query failed
        while snowflake_conn.is_still_running(snowflake_conn.get_query_status_throw_if_error(query_id)):
            print("The query is still running. Sleeping for 15 seconds")
            time.sleep(15)
        
        log.info(f"The table has been unloaded successfully to {gcs_destination}")

        cur.get_results_from_sfqid(query_id)
        rows_unloaded, input_bytes, output_bytes = cur.fetchone()
        view_trigger_mandatory_mapping = eval(Variable.get("view_trigger_mandatory_mapping", default_var="{}"))
        if rows_unloaded == 0:
            if not view_trigger_mandatory_mapping.get(self.table, False):
                log.warn(f"The query {table_expression} unloaded zero rows but the view {self.table} is non-mandatory")
                return
            else:
                raise AirflowException(f"The view {self.table} is mandatory but the query {table_expression} unloaded zero rows")
        else:
            log.info(f"Total rows unloaded {rows_unloaded}")

        # if snowflake_conn.get_query_status(query_id).name == 'SUCCESS':
        #     log.info(f"The table has been unloaded successfully to {self.gcs_destination}")
        # elif snowflake_conn.get_query_status(query_id).name in ['FAILED','FAILED_WITH_ERROR']:
        #     log.error(f"The data unloading from table has failed")

        #bq_schema = self._get_bq_schema()

        # Get the query result metadata
        log.info(f"Fetching the query result metadata")
        query_result_metadata = cur.describe(table_expression)

        # log.info(f"Transforming Snowflake schema to bigquery schema")
        # bq_schema = self._get_bq_schema_from_snowflake_query_result_metadata(query_result_metadata)
        # log.info(f"Snowflake schema:\n{query_result_metadata}\nBigQuery Schema:\n{bq_schema}")
        log.info("Creating the bigquery schema")
        bq_schema = self._get_bq_schema(gcs_destination, query_result_metadata) 
        log.info(f"Snowflake schema:\n{query_result_metadata}\nBigQuery Schema:\n{bq_schema}")
        
        log.info(f"Running the DDL to create the table based on the replace parameter")
        self._create_table(bq_schema, replace=self.replace_destination_table, partition_column=self.bq_partition_column, clustering_columns=self.bq_clustering_columns)

        if self.staging_file_format.upper() == 'PARQUET':
            uris = [f"{gcs_destination}*.parquet"]
        elif self.staging_file_format.upper() == 'CSV':
            uris = [f"{gcs_destination}*.csv.gz"]
        else:
            raise Exception(f"{self.staging_file_format} file format is not supported.")

        self._load_job_with_query(bq_schema, uris, source_format=self.staging_file_format, field_delimiter=self.staging_file_delimiter)

        # Delete the staging blobs
        gcs_client = storage.Client()
        blob_path_components = gcs_destination.replace("gs://","").split("/")
        bucket_name, blob_prefix =  blob_path_components[0:2]
        bucket = gcs_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=blob_prefix)
        for blob in blobs:
            blob.delete()


    
    def _get_bq_schema(self, parquet_file_location:str=None, snowflake_query_result_metadata:List[ResultMetadata]=None):
        # If file format is parquet, it's better to get it from parquet file schema
        if self.staging_file_format.upper() == 'PARQUET':
            assert parquet_file_location is not None and parquet_file_location.startswith('gs://'),\
                f'parquet_file_location {parquet_file_location} should be a GCS location'
            gcs_client = storage.Client()
            bucket_name = parquet_file_location.replace('gs://','').split('/')[0]
            prefix = "/".join(parquet_file_location.replace('gs://','').split('/')[1:])
            bucket = gcs_client.get_bucket(bucket_name)
            sample_blobs = list(bucket.list_blobs(prefix=prefix, max_results=5))
            if len(sample_blobs) < 1:
                raise Exception(f"Couldn't find any objects with prefix {prefix}")
            log.info(f"Using object {sample_blobs[0]} to infer schema")
            bq_schema = self._get_bq_schema_from_gcs(f"gs://{bucket_name}/{sample_blobs[0].name}")
            return bq_schema            
        else:
            # For other file formats, directly map from snowflake to bigquery schema
            assert snowflake_query_result_metadata is not None, 'snowflake_query_result_metadata is None'
            log.info(f"Transforming Snowflake schema to bigquery schema")
            bq_schema = self._get_bq_schema_from_snowflake_query_result_metadata(snowflake_query_result_metadata)
            log.info(f"Snowflake schema:\n{snowflake_query_result_metadata}\nBigQuery Schema:\n{bq_schema}")
            return bq_schema
    
    def _get_bq_schema_from_gcs(self, gcs_path:str) -> List[Dict]:
        schema = []
        if gcs_path.endswith('parquet'):
            gcs_fs = gcsfs.GCSFileSystem()
            with gcs_fs.open(gcs_path, 'rb') as f:
                parquet_file = pq.ParquetFile(f)
            schema = self._convert_parquet_schema_to_bq(parquet_file.schema)
        elif gcs_path.endswith(".gz"):
            gcs_fs = gcsfs.GCSFileSystem()
            with gcs_fs.open(gcs_path,"rb") as f:
                decompressed_bytes = zlib.decompress(f.read(), zlib.MAX_WBITS|32)[:10240]
                file_header = str(decompressed_bytes, 'UTF-8').split('\n')[0]
                if file_header.count('|') >= file_header.count(','):
                    delimiter = '|'
                else:
                    delimiter =  ','
                columns = file_header.split(delimiter)
                for col in columns:
                    schema.append({"name": re.sub('[^A-Za-z0-9_]','_',col), "type":"STRING"})
        else:
            gcs_fs = gcsfs.GCSFileSystem()
            with gcs_fs.open(gcs_path,"rb") as f:
                file_header = str(f.readline(), 'UTF-8').split('\n')[0]
                if file_header.count('|') >= file_header.count(','):
                    delimiter = '|'
                else:
                    delimiter =  ','
                columns = file_header.split(delimiter)
                for col in columns:
                    schema.append({"name": re.sub('[^A-Za-z0-9_]','_',col), "type":"STRING"})
        return schema
    
    def _convert_parquet_schema_to_bq(self, parquet_schema) -> List[Dict]:
        #https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#type_conversions
        parquet_to_bq_type_map = {
            "BOOLEAN" : {
                "NONE" : "BOOLEAN"
            },
            "INT32" : {
                "NONE" : "INTEGER",
                "INTEGER" : "INTEGER",
                "UINT_8" : "INTEGER",
                "UINT_16" : "INTEGER",
                "UINT_32" : "INTEGER",
                "INT_8" : "INTEGER",
                "INT_16" : "INTEGER",
                "INT_32" : "INTEGER",
                "DECIMAL" : "NUMERIC",
                "DATE" : "DATE",
                "INT" : "INTEGER"
            },
            "INT64" : {
                "NONE" : "INTEGER",
                "INTEGER" : "INTEGER",
                "UINT_64" : "INTEGER",
                "INT_64" : "INTEGER",
                "DECIMAL" : "NUMERIC",
                "TIMESTAMP" : "TIMESTAMP"
            },
            "INT96" : {
                "NONE" : "TIMESTAMP"
            },
            "FLOAT" : {
                "NONE" : "FLOAT64"
            },
            "DOUBLE" : {
                "NONE" : "FLOAT64"
            },
            "BYTE_ARRAY" : {
                "NONE" : "BYTES",
                "STRING" : "STRING",
                "STRING (UTF8)" : "STRING"
            },
            "FIXED_LEN_BYTE_ARRAY" : {
                "NONE" : "BYTES",
                "DECIMAL" : "BIGNUMERIC"
            }
        }
        num_cols = len(parquet_schema.names)
        bq_schema:List[Dict] = []
        for i in range(num_cols):
            parquet_type = parquet_schema.column(i).physical_type.upper()
            parquet_logical_type = parquet_schema.column(i).logical_type.type.upper()
            log.info(f"parquet type: {parquet_type} , parquet_logical_type: {parquet_logical_type}")
            bq_type = parquet_to_bq_type_map[parquet_type][parquet_logical_type]
            bq_schema.append({"name": re.sub('[^A-Za-z0-9_]','_', parquet_schema.column(i).name), "type": bq_type})
        return bq_schema
    
    def _get_file_format_expression(self):
        if self.staging_file_format.upper() == 'PARQUET':
            return '(TYPE=PARQUET COMPRESSION=SNAPPY)'
        elif self.staging_file_format.upper() == 'CSV':
            return '(TYPE=CSV COMPRESSION=GZIP)'
        else:
            raise NotImplementedError(f"{self.staging_file_format} file format is not supported.")
    
    def _get_overwrite_expression(self):
        if self.overwrite_files:
            return "TRUE"
        else:
            return "FALSE"
    
    def _get_bq_schema_from_snowflake_query_result_metadata(self, res_metadata:List[ResultMetadata]):
        snowflake_type_code_to_bq_type = {
            0 : "BIGINT",
            1 : "NUMERIC",
            2 : "STRING",
            3 : "DATE",
            4 : "TIMESTAMP",
            7 : "TIMESTAMP",
            8 : "DATETIME",
            11 : "BYTES",
            12 : "TIME",
            13 : "BOOL"
        }
        bq_schema = []
        for res_md in res_metadata:
            bq_schema.append({"name":res_md.name, "type": snowflake_type_code_to_bq_type.get(res_md.type_code)})
        return bq_schema
    
    def _check_audit_column(self, res_metadata:List[ResultMetadata], audit_column_name:str):
        for res_md in res_metadata:
            if res_md.name.upper() == audit_column_name.upper():
                return True
        return False

    def _create_table(self, schema: List[Dict], replace=True, partition_column=None, clustering_columns=None,
                      require_partition_filter=False):
        from airflow.models import Variable
        ddl_query = self._create_ddl_from_schema(schema, replace, partition_column, clustering_columns)
        log.info(ddl_query)
        bq_client = bigquery.Client(location=self.bigquery_region)
        bq_job = bq_client.query(ddl_query)
        log.info(f"Job Id: {bq_job.job_id}")
        bq_job.result()
        while not bq_job.done():
            time.sleep(2)
            log.info(f"Still running Job {bq_job.job_id}")
        if bq_job.done():
            log.info(f"Successfully created table {self.bq_project}.{self.bq_dataset}.{self.bq_table}")
        else:
            raise RuntimeError(f"""Error occured when creating table {self.bq_project}.{self.bq_dataset}.{self.bq_table}.
            Refer to Job Id: {bq_job.job_id} for more details""")
    
    def _create_ddl_from_schema(self, schema: List[Dict], replace=True, partition_column=None, clustering_columns=None, 
                                require_partition_filter=False) -> str:
        column_definitions = ""
        for col in schema:
            if 'default_value_expression' not in col:
                column_definitions += f"{col['name']} {col['type']},\n"
            else:
                default_value = self.get_default_val_for_bq_type(col['type'])
                if default_value:
                    column_definitions += f"{col['name']} {col['type']} DEFAULT {default_value},\n"
                else:
                    column_definitions += f"{col['name']} {col['type']},\n"
        column_definitions = re.sub(',\n$','', column_definitions)
        query = f"""CREATE {'OR REPLACE' if replace else ''} TABLE {'' if replace else 'IF NOT EXISTS'} 
        {self.bq_project}.{self.bq_dataset}.{self.bq_table}\n(\n{column_definitions}\n)\n"""
        if partition_column:
            query += f"PARTITION BY DATE({partition_column})\n"
        if clustering_columns:
            query += f"CLUSTER BY {','.join(clustering_columns)}\n"
        if require_partition_filter:
            query += "OPTIONS(require_partition_filter=false)"
        return query

    def _load_job_with_query(self, schema:List[Dict], uris:List[str], source_format:str, field_delimiter:str=None):
        columns_to_exclude = []
        columns = ''
        for col in schema:
            if col['name'] not in columns_to_exclude:
                columns += f"{col['name']} {col['type']},"
        columns = re.sub(',$','', columns)
        if source_format.upper() == 'PARQUET':
            query = f"""LOAD DATA INTO {self.bq_project}.{self.bq_dataset}.{self.bq_table}\n({columns})\n
            FROM FILES (
                uris = {uris},
                format = "{source_format}"
            )
            """
            log.info(query)
        elif source_format.upper() == 'CSV':
            query = f"""LOAD DATA INTO {self.bq_project}.{self.bq_dataset}.{self.bq_table}\n({columns})\n
            FROM FILES (
                uris = {uris},
                format = "{source_format}",
                field_delimiter = "{field_delimiter}",
                skip_leading_rows = 1
            )
            """
            log.info(query)
        else:
            raise NotImplementedError(f"source_format={source_format} is not supported")
        
        try:
            bq_job = self.bq_client.query(query)
            log.info(f"Job Id: {bq_job.job_id}")
            bq_job.result()
        except Exception as e:
            errors = bq_job.errors
            log.error(f"#### Errors for Job {bq_job.job_id} ####\n{errors}")
            log.error(e)
            raise AirflowException(f"Load job {bq_job.job_id} failed due to following errors {errors}")
        else:
            log.info(f"Job {bq_job.job_id} is successful")
            return True


    def get_default_val_for_bq_type(self, type: str):
        if type == 'DATETIME':
            return 'CURRENT_DATETIME()'
        if type == 'DATE':
            return 'CURRENT_DATE()'
        if type == 'TIMESTAMP':
            return 'CURRENT_TIMESTAMP()'
        if type == 'TIME':
            return 'CURRENT_TIME()'
        else:
            return None