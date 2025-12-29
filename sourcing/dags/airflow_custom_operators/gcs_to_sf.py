import os
import re
import time
import gzip
from datetime import datetime
from typing import Dict, List
from google.cloud import bigquery
from google.cloud import storage
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dataflow_options.utils import Logger

import gcsfs
import pyarrow.parquet as pq
import pandas as pd
from typing import List, Dict, Union

import pytz
from airflow.models import Variable
from airflow.exceptions import AirflowException

log = Logger(__name__)

class GCSToSnowflakeOperator(BaseOperator):

    @apply_defaults
    def __init__(self, project_id: str,billing_project: str, table_id: str, bucket_name:str, blob_prefix:str='', prefix:str=None, suffix:str=None, 
                 start_offset:str=None, end_offset=None, replace:bool = True, clustering_columns:List[str]=None, 
                 use_blob_prefix_with_offsets=False, move_files_to_archive=False, field_delimiter:str='|', 
                 pull_type:str='full', directories:List[str]=None, folder_date_pattern:str='%Y-%m-%d', bigquery_region="US", *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.billing_project = billing_project
        self.table_id = table_id
        self.bucket_name = bucket_name
        self.blob_prefix = blob_prefix
        self.prefix = prefix # prefix will be view name (ex: ia_transaction)
        self.suffix = suffix # suffix will be the file format
        self.start_offset = start_offset # start_offset will be the date (YYYY-MM-DD)
        self.end_offset = end_offset
        self.use_blob_prefix_with_offsets = use_blob_prefix_with_offsets
        self.replace = replace
        
        self.clustering_columns = clustering_columns
        self.move_files_to_archive = move_files_to_archive
        self.field_delimiter = field_delimiter
        self.pull_type = pull_type
        self.directories = directories # directories will be passed based on the trigger
        self.folder_date_pattern = folder_date_pattern
        self.bigquery_region = bigquery_region
    
    def execute(self, context):
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(self.bucket_name)
        prefixes = self._get_prefixes()
        if self.pull_type.lower() == 'full' and len(prefixes)>0:
            prefixes = [sorted(prefixes, reverse=True)[0]]
        log.info(f"prefixes: {prefixes}")
        view_trigger_mandatory_mapping = eval(Variable.get("view_trigger_mandatory_mapping", default_var="{}"))
        if len(prefixes) > 0:
            schema = None
            for prefix in prefixes:
                sample_prefix = prefix.rstrip("/")+"/" if prefix and len(prefix)>0 else ""
                sample_blobs = list(bucket.list_blobs(prefix=f"{sample_prefix}{self.prefix.rstrip('/')}", max_results=100))\
                    +list(bucket.list_blobs(prefix=f"{sample_prefix}{self.prefix.rstrip('/')}/", max_results=100))
                blob_name_pattern = f"{sample_prefix.rstrip('/')}/{'' if self.prefix.endswith('/') else f'{self.prefix}'}.*.{self.suffix}$"
                log.info(f"blob_name_pattern: {blob_name_pattern}")
                if len(sample_blobs) == 0:
                    if not view_trigger_mandatory_mapping.get(self.table_id, False):
                        log.warn(f"There are no blobs with pattern {blob_name_pattern}. Checking the blobs with different date prefixes")        
                        continue
                    else:
                        raise AirflowException(f"There are no blobs with pattern {blob_name_pattern}. View {self.table_id} is mandatory. So pipeline will not proceed with missing dates")

                
                for blob in sample_blobs:
                    #if re.match(f'.*{self.suffix}$', blob.name):
                    if re.match(blob_name_pattern, blob.name):
                        schema = self._get_schema(f"gs://{self.bucket_name}/{blob.name}")
                        file_name = f"gs://{self.bucket_name}/{blob.name}"
                        file_suffixes = []
                        __file_suffix = True
                        while __file_suffix != "":
                            file_name, __file_suffix = os.path.splitext(file_name)
                            if __file_suffix:
                                file_suffixes.append(__file_suffix)
                        file_suffix = ""
                        while len(file_suffixes):
                            file_suffix += file_suffixes.pop()
                        break
                    else:
                        log.info(f"blob_name_pattern {blob_name_pattern} did not match blob name {blob.name}")
            
            log.info(f"Schema: {schema}")

            if not schema and not view_trigger_mandatory_mapping.get(self.table_id, False):
                log.info("no file found to fetch schema")
                return
            
            assert schema is not None and len(schema)>0, f"schema is either None or []. Check the if the file format specified {self.suffix} is valid or files are missing"
            log.info(f"Running DDL to create table")
            self._create_table(schema=schema, replace=self.replace,
                                        clustering_columns=self.clustering_columns)
            
            initial_row_count = self._get_table_row_count(self.table_id)
            log.info(f"Initial row count {initial_row_count}")
            
            for prefix in sorted(prefixes):
                blob_name_pattern = f"{prefix.rstrip('/')}/{'' if self.prefix.endswith('/') else f'{self.prefix}'}.*.{self.suffix}"
                log.info(f"Loading all blobs with gs://{self.bucket_name}/{blob_name_pattern}")
                load_job_status, load_job_error_message = self._load_to_sf_with_prefix(schema, prefix, suffix=file_suffix, field_delimiter = self.field_delimiter)
                if load_job_status:
                    #log.info(f"Successfully loaded all blobs with prefix gs://{self.bucket_name}/{prefix} to sf")
                    log.info(f"Successfully loaded all blobs with pattern gs://{self.bucket_name}/{blob_name_pattern} to sf")
                    row_count = self._get_table_row_count(self.table_id)
                    log.info(f"Row count after loading files {prefix}: {row_count}")
                    if row_count - initial_row_count == 0:
                        # If there is an empty file, zero rows will be loaded. Hence proceed with next file instead of updating syncstartdatetime
                        log.info(f"0 Rows loaded from {prefix}")
                        continue
                    initial_row_count = row_count
                    file_date = self._parse_file_date(prefix)
                    if file_date:
                        log.info(f"Updating SYNCSTARTDATETIME with {file_date} for all the rows loaded from blobs gs://{self.bucket_name}/{prefix}")
                        self._update_syncstartdatetime(file_date)
                    else:
                        log.info(f"""Unable to parse date from the prefix '{prefix}'. Leaving the SYNCSTARTDATETIME at CURRENT_DATETIME()
                        for all the rows loaded from blobs gs://{self.bucket_name}/{prefix}""")
                else:
                    log.error(f"Failed loading blobs with prefix gs://{self.bucket_name}/{prefix} to sf. Refer to job ids in the logs for more info")
                    if not view_trigger_mandatory_mapping.get(self.table_id, False) and re.search('No file can be matched with URI',load_job_error_message.message):
                        log.error(f"Failed loading blobs with prefix gs://{self.bucket_name}/{prefix} to sf. Refer to job ids in the logs for more info")
                    else:
                        raise AirflowException(f"Failed loading blobs with prefix gs://{self.bucket_name}/{prefix} to sf. Refer to job ids in the logs for more info")
                        

    def _get_schema(self, gcs_path:str) -> List[Dict]:
        schema = []
        if gcs_path.endswith('parquet'):
            gcs_fs = gcsfs.GCSFileSystem()
            with gcs_fs.open(gcs_path, 'rb') as f:
                parquet_file = pq.ParquetFile(f)
            schema = self._convert_parquet_schema_to_sf(parquet_file.schema)
            # columns = parquet_file.schema.names
            # for column in columns:
            #     schema.append({"name":re.sub('[^A-Za-z0-9_]','_',column), "type":"STRING"})
            schema.append({"name": "SYNCSTARTDATETIME", "type": "DATETIME", "default_value_expression": "CURRENT_DATETIME()"})
            return schema
        elif gcs_path.endswith(".gz"):
            log.info(f"Detected compressed file: {gcs_path}")
            gcs_fs = gcsfs.GCSFileSystem()
            # Define the chunk size
            chunk_size = 1024 * 1024  # 1 MB

            # Initialize the decompressed data buffer
            decompressed_data = bytearray()

            # Open the GCS file
            with gcs_fs.open(gcs_path, "rb") as f:
                # Initialize the decompressor
                decompressor = gzip.GzipFile(fileobj=f)
                # Read and decompress the file in chunks
                while True:
                    # Read a chunk of compressed data
                    chunk = decompressor.read(chunk_size)
                    if not chunk:
                        break  # End of file

                    # Decompress the chunk and append it to the decompressed data buffer
                    decompressed_data.extend(chunk)

                    # Break if the decompressed data exceeds 10240 bytes
                    if len(decompressed_data) >= 10240:
                        break
                decompressed_bytes = decompressed_data[:10240]
                file_header = str(decompressed_bytes, 'UTF-8').split('\n')[0]
                log.info(f"File header: {file_header}")
                if file_header.count('|') >= file_header.count(','):
                    delimiter = '|'
                else:
                    delimiter =  ','
                columns = file_header.split(delimiter)
                for col in columns:
                    col = col.replace('"', '').replace("'", '')
                    schema.append({"name": re.sub('[^A-Za-z0-9_]','_',col), "type":"STRING"})
                schema.append({"name": "SYNCSTARTDATETIME", "type": "DATETIME", "default_value_expression": "CURRENT_DATETIME()"})
                return schema
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
                    col = col.replace('"', '').replace("'", '')
                    schema.append({"name": re.sub('[^A-Za-z0-9_]','_',col), "type":"STRING"})
                schema.append({"name": "SYNCSTARTDATETIME", "type": "DATETIME", "default_value_expression": "CURRENT_DATETIME()"})
                return schema
    
    def _create_ddl_from_schema(self, schema: List[Dict], replace=True, clustering_columns=None) -> str:
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        complete_table_name = wc._get_complete_table_name(self.table_id, True)
        column_definitions = ""
        for col in schema:
            if 'default_value_expression' not in col:
                column_definitions += f"{col['name']} {col['type']},\n"
            else:
                default_value = self.get_default_val_for_sf_type(col['type'])
                if default_value:
                    column_definitions += f"{col['name']} {col['type']} DEFAULT {default_value},\n"
                else:
                    column_definitions += f"{col['name']} {col['type']},\n"
        column_definitions = re.sub(',\n$','', column_definitions)
        query = f"""CREATE {'OR REPLACE' if replace else ''} TABLE {'' if replace else 'IF NOT EXISTS'} 
        {complete_table_name}\n(\n{column_definitions}\n)\n"""
        if clustering_columns:
            query += f"CLUSTER BY {','.join(clustering_columns)}\n"
        return query
    
    def get_default_val_for_sf_type(self, type: str):
        if type == 'DATETIME':
            return 'CURRENT_TIMESTAMP()::DATETIME'
        if type == 'DATE':
            return 'CURRENT_DATE()'
        if type == 'TIMESTAMP':
            return 'CURRENT_TIMESTAMP()'
        if type == 'TIME':
            return 'CURRENT_TIME()'
        else:
            return None
    
    def _create_table(self, schema: List[Dict], replace=True, clustering_columns=None):
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        
        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        ddl_query = self._create_ddl_from_schema(schema, replace, clustering_columns)
        log.info(ddl_query)
        wc.execute_query(ddl_query, False)
        log.info(f"Successfully created table {self.table_id}")
   
    def _parse_file_date(self, path:str) -> Union[str,None]:
        date_regex_patterns_and_formats = [('\d{14}','%Y%m%d%H%M%S'),('\d{12}','%Y%m%d%H%M'),('\d{8}','%Y%m%d'),('\d{4}-\d{2}-\d{2}','%Y-%m-%d')]
        for date_pattern in date_regex_patterns_and_formats:
            matched = re.search(date_pattern[0], path)
            if matched:
                dt = datetime.strptime(matched.group(0), date_pattern[1])
                return datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')
        return None

    def _update_syncstartdatetime(self, dt:str):
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        complete_table_name = wc._get_complete_table_name(self.table_id, True)
        
        max_timestamp_query = f"""SELECT MAX(SYNCSTARTDATETIME) as syncstartdatetime FROM {complete_table_name}"""
        log.info(f"max timestamp query: {max_timestamp_query}")
        df = wc.execute_query(max_timestamp_query, True)
        max_timestamp = str(df.syncstartdatetime[0])
        query = f"""UPDATE {complete_table_name} 
        SET SYNCSTARTDATETIME='{dt}'
        WHERE SYNCSTARTDATETIME IN ('{max_timestamp}')"""
        log.info(query)
        wc.execute_query(max_timestamp_query, False)
        log.info(f"Successfully updated SYNCSTARTDATETIME in {self.table_id}")
    
    def _get_prefixes(self) -> List[str]:
        gcs_client:storage.Client = storage.Client()
        bucket:storage.Bucket = gcs_client.bucket(self.bucket_name)
        prefixes = []
        if self.directories and len(self.directories)>=0:
            for directory in self.directories:
                log.info(f"directory: {directory}", )
                prefix = f"{'/'.join(directory.replace('gs://','').split('/')[1:])}{self.prefix if self.prefix.endswith('/') else ''}"
                log.info(f"bucket:{self.bucket_name}, prefix: {prefix}")
                if len(list(bucket.list_blobs(prefix=prefix, max_results=5))) != 0:
                    prefixes.append(prefix)
        else:
            log.info(f"Using the folder_date_pattern {self.folder_date_pattern}")
            date_ranges = pd.date_range(start=self.start_offset, end=datetime.now(pytz.timezone(Variable.get('dag_timezone', default_var='UTC'))).strftime('%Y-%m-%d'))
            for dt in date_ranges: 
                prefix_name = f"{self.blob_prefix}/{dt.strftime(self.folder_date_pattern)}/{self.prefix if self.prefix.endswith('/') else ''}"
                prefix_name = prefix_name.lstrip('/')
                log.info(f"Prefixs looking for : {prefix_name}")
                if len(list(bucket.list_blobs(prefix=prefix_name, max_results=5))) != 0:
                    prefixes.append(prefix_name)
                # if len(list(bucket.list_blobs(prefix=f"{self.blob_prefix}/{dt.strftime(self.folder_date_pattern)}/{self.prefix if self.prefix.endswith('/') else ''}", max_results=5))) != 0:
                #     prefixes.append(f"{self.blob_prefix}/{dt.strftime('%Y%m%d')}/{self.prefix if self.prefix.endswith('/') else ''}")
        return prefixes

    def _load_to_sf_with_prefix(self, schema:List[Dict], prefix:str, suffix:str, field_delimiter:str='|'):
        # parquet_file_uris = [f"gs://{self.bucket_name}/{prefix}/*.parquet"]
        # csv_file_uris = [f"gs://{self.bucket_name}/{prefix}/*.csv",f"gs://{self.bucket_name}/{prefix}/*.txt",
        #          f"gs://{self.bucket_name}/{prefix}/*.dat",f"gs://{self.bucket_name}/{prefix}/*.csv.gz",
        #          f"gs://{self.bucket_name}/{prefix}/*.txt.gz",f"gs://{self.bucket_name}/{prefix}/*.dat.gz"]
        prefix = prefix.rstrip("/")
        if re.search('parquet', suffix):
            uris = [f"gs://{self.bucket_name}/{prefix}/*.parquet"] if self.prefix.endswith("/") else \
                [f"gs://{self.bucket_name}/{prefix}/{self.prefix}*.parquet"]
            return self._load_job_with_query(schema, uris=uris, source_format='PARQUET')
        elif re.search('(csv|dat|txt|csv.gz|dat.gz)', suffix):
            uris = [f"gs://{self.bucket_name}/{prefix}/*.csv",f"gs://{self.bucket_name}/{prefix}/*.txt",
                 f"gs://{self.bucket_name}/{prefix}/*.dat",f"gs://{self.bucket_name}/{prefix}/*.csv.gz",
                 f"gs://{self.bucket_name}/{prefix}/*.txt.gz",f"gs://{self.bucket_name}/{prefix}/*.dat.gz"] if self.prefix.endswith("/") else \
                [f"gs://{self.bucket_name}/{prefix}/{self.prefix}*.csv",f"gs://{self.bucket_name}/{prefix}/{self.prefix}*.txt",
                 f"gs://{self.bucket_name}/{prefix}/{self.prefix}*.dat",f"gs://{self.bucket_name}/{prefix}/{self.prefix}*.csv.gz",
                 f"gs://{self.bucket_name}/{prefix}/{self.prefix}*.txt.gz",f"gs://{self.bucket_name}/{prefix}/{self.prefix}*.dat.gz"]
            return self._load_job_with_query(schema, uris=uris, source_format='CSV', field_delimiter=field_delimiter)
        else:
            raise Exception(f"Given suffix {suffix} doesn't match with configured file formats")

    
    def _load_job_with_query(self, schema:List[Dict], uris:List[str], source_format:str, field_delimiter:str=None):
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        complete_table_name = wc._get_complete_table_name(self.table_id, True)  
        columns_to_exclude = ['SYNCSTARTDATETIME']
        columns = ''
        for col in schema:
            if col['name'] not in columns_to_exclude:
                columns += f"{col['name']} {col['type']},"
        columns = re.sub(',$','', columns)
        if source_format.upper() == 'PARQUET':
            query = f"""
                COPY INTO {complete_table_name}
                FROM {uris}
                STORAGE_INTEGRATION = {warehouse_kwargs['storage_integration']}
                FILE_FORMAT = (TYPE = 'PARQUET', MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE');
            """
            self.log.info(query)
        elif source_format.upper() == 'CSV':
            query = f"""
                COPY INTO {complete_table_name}
                FROM {uris}
                STORAGE_INTEGRATION = {warehouse_kwargs['storage_integration']}
                FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '{field_delimiter}', SKIP_HEADER = 1, PARSE_HEADER = TRUE, MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE');
            """
            log.info(query)
        else:
            raise Exception(f"source_format={source_format} is not supported")
        
        wc.execute_query(query, False)
        log.info("Job is successful")
        return True, None
    
    def convert_parquet_schema_to_snowflake(self, parquet_schema) -> List[Dict]:
        # Mapping of Parquet types to Snowflake types
        # Reference: https://docs.snowflake.com/en/user-guide/script-data-type-parquet
        parquet_to_snowflake_type_map = {
            "BOOLEAN": {
                "NONE": "BOOLEAN"
            },
            "INT32": {
                "NONE": "INT",
                "INTEGER": "INT",
                "UINT_8": "INT",
                "UINT_16": "INT",
                "UINT_32": "INT",
                "INT_8": "INT",
                "INT_16": "INT",
                "INT_32": "INT",
                "DECIMAL": "NUMBER",
                "DATE": "DATE",
                "INT": "INT"
            },
            "INT64": {
                "NONE": "INT",
                "INTEGER": "INT",
                "UINT_64": "INT",
                "INT_64": "INT",
                "DECIMAL": "NUMBER",
                "TIMESTAMP": "TIMESTAMP",
                "INT": "INT"
            },
            "INT96": {
                "NONE": "TIMESTAMP"
            },
            "FLOAT": {
                "NONE": "FLOAT"
            },
            "DOUBLE": {
                "NONE": "DOUBLE"
            },
            "BYTE_ARRAY": {
                "NONE": "BINARY",
                "STRING": "VARCHAR",
                "STRING (UTF8)": "VARCHAR"
            },
            "FIXED_LEN_BYTE_ARRAY": {
                "NONE": "BINARY",
                "DECIMAL": "NUMBER"
            }
        }
        
        num_cols = len(parquet_schema.names)
        snowflake_schema: List[Dict] = []
        
        for i in range(num_cols):
            parquet_type = parquet_schema.column(i).physical_type.upper()
            parquet_logical_type = parquet_schema.column(i).logical_type.type.upper()
            log.info(f"parquet type: {parquet_type}, parquet_logical_type: {parquet_logical_type}")
            
            snowflake_type = parquet_to_snowflake_type_map[parquet_type][parquet_logical_type]
            
            # Clean column name to comply with Snowflake naming conventions
            column_name = re.sub('[^A-Za-z0-9_]', '_', parquet_schema.column(i).name).upper()
            
            snowflake_schema.append({
                "name": column_name,
                "type": snowflake_type
            })
        
        return snowflake_schema
    
    def _get_table_row_count(self, table_id:str):
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        return wc._get_row_count(table_id)
        
