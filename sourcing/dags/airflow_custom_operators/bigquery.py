import re
import time
import json
from datetime import datetime
from typing import Dict, List
from google.cloud import storage
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dataflow_options.utils import Logger
from airflow_options.gcp_utils import BigQuery

logger = Logger(__name__)
bq_client = BigQuery(location=Variable.get("bigquery_region", default_var="US"))


def get_schema_from_gcs(gcs_path: str):
    gcs_client = storage.Client()
    bucket_name = gcs_path.replace('gs://','').split('/')[0]
    blob_name = "/".join(gcs_path.replace('gs://','').split('/')[1:])
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    schema_as_bytes = blob.download_as_bytes().decode('UTF-8')
    schema_as_list = json.loads(schema_as_bytes)
    # schema = []
    # for col in schema_as_list:
    #     schema.append(bigquery.SchemaField(col['name'], col['type']))
    return schema_as_list

def get_blobs_list_with_pattern(pattern:str):
    gcs_client = storage.Client()
    bucket_name = pattern.replace('gs://','').split('/')[0]
    blob_prefix = "/".join(pattern.replace('gs://','').split('/')[1:-1])
    blobs = gcs_client.list_blobs(bucket_name, prefix=blob_prefix)
    matched_blobs = []
    for blob in blobs:
        if re.match(pattern, f"gs://{bucket_name}/{blob.name}"):
            matched_blobs.append(f"gs://{bucket_name}/{blob.name}")
    return matched_blobs

def validate_uris(uris: List[str]):
    gcs_client = storage.Client()
    uri = uris[0]
    bucket_name = uri.replace('gs://','').split('/')[0]
    blob_prefix = "/".join(uri.replace('gs://','').split('/')[1:-1])
    bucket = gcs_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=blob_prefix))
    if len(blobs) > 0:
        logger.info(f"Given URI prefixes {uris} exists ")
        return True
    else:
        logger.warn(f"Given URI prefixes {uris} doesn't exists ")
        return False
        

def get_default_val_for_bq_type(type: str):
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

def create_ddl_from_schema(project_id: str, dataset_id: str, table_id: str, schema: List[Dict], replace:bool=True, partition_column=None, clustering_columns=None, 
require_partition_filter=False):
    column_definitions = ""
    for col in schema:
        if 'default_value_expression' not in col:
            column_definitions += f"{col['name']} {col['type']},\n"
        else:
            default_value = get_default_val_for_bq_type(col['type'])
            if default_value:
                column_definitions += f"{col['name']} {col['type']} DEFAULT {default_value},\n"
            else:
                column_definitions += f"{col['name']} {col['type']},\n"
    column_definitions = re.sub(',\n$','', column_definitions)
    #query = f"CREATE {'OR REPLACE' if replace else ''} TABLE {project_id}.{dataset_id}.{table_id}\n(\n{column_definitions}\n)\n"
    query = f"CREATE {'OR REPLACE' if replace else ''} TABLE {'' if replace else 'IF NOT EXISTS'} {project_id}.{dataset_id}.{table_id}\n(\n{column_definitions}\n)\n"
    if partition_column:
        query += f"PARTITION BY DATE({partition_column})\n"
    if clustering_columns:
        query += f"CLUSTER BY {','.join(clustering_columns)}\n"
    if require_partition_filter:
        query += f"OPTIONS(require_partition_filter=false)"
    return query

def create_load_data_query(project_id: str, dataset_id: str, table_id: str, schema: List[Dict], uris: List[str], source_format='CSV', field_delimiter='|'):
    columns_to_exclude = ['SYNCSTARTDATETIME','FILE_DATE']
    columns = ''
    for col in schema:
        if col['name'] not in columns_to_exclude:
            columns += f"{col['name']} {col['type']},"
    columns = re.sub(',$','', columns)
    query = f"""LOAD DATA INTO {project_id}.{dataset_id}.{table_id}\n({columns})\n
    FROM FILES (
        uris = {uris},
        format = "{source_format}",
        field_delimiter = "{field_delimiter}",
        skip_leading_rows = 1
    )
    """
    return query

def parse_file_date(file_date:str) -> str:
    patterns = ['%Y%m%d%H%M%S','%Y%m%d']
    for pattern in patterns:
        try:
            dt = datetime.strptime(file_date, pattern)
            return datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.warn(f"{file_date} didn't match with date format {pattern}")
    
    raise Exception(f"file_date {file_date} did not match with any of the given formats {patterns}") 


class ZeroRowsLoadedException(Exception):
    pass

class BigQueryCreateAndLoadTablev2(BaseOperator):

    @apply_defaults
    def __init__(self, project_id: str, dataset_id: str, table_id: str, uris: str, schema_fields: str=None, gcs_schema_object: str=None, 
        replace:bool = True, partition_column:str=None, clustering_columns:List[str]=None, require_partition_filter:bool=False, 
        field_delimiter:str='|', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.gcs_schema_object = gcs_schema_object
        self.uris = uris
        self.replace = replace
        self.partition_column = partition_column
        self.clustering_columns = clustering_columns
        self.require_partition_filter = require_partition_filter
        self.field_delimiter = field_delimiter
    
    def execute(self, context):
        try:
            if validate_uris(self.uris):
                if self.schema_fields is None and self.gcs_schema_object is None:
                    raise AirflowException(f"Either schema_fields or gcs_schema_object should be provided.")
                if self.schema_fields:
                    schema = self.schema_fields
                elif self.gcs_schema_object:
                    schema = get_schema_from_gcs(self.gcs_schema_object)
                
                ddl_query = create_ddl_from_schema(self.project_id, self.dataset_id, self.table_id, schema, replace=self.replace, 
                partition_column=self.partition_column, clustering_columns=self.clustering_columns, require_partition_filter=self.require_partition_filter)
                logger.info(f"Executing DDL {ddl_query}")
                run_ddl_job = bq_client.query(ddl_query)
                logger.info(f"Job Id: {run_ddl_job.job_id}")
                run_ddl_job.result()
                while not run_ddl_job.done():
                    time.sleep(2)
                    logger.info(f"Still running Job {run_ddl_job.job_id}")
                if run_ddl_job.done():
                    logger.info(f"Successfully created table {self.project_id}.{self.dataset_id}.{self.table_id}")

                
                file_date_found = True
                for uri in self.uris:
                    logger.info(f"Finding files with pattern {uri}")
                    matched_blobs = get_blobs_list_with_pattern(uri)
                    logger.info(f"Found {len(matched_blobs)} files {matched_blobs}")
                    for blob in sorted(matched_blobs):
                        logger.info(f"Loading {blob} into {self.project_id}.{self.dataset_id}.{self.table_id}")
                        file_date_match = re.search("\d{8,14}", blob.split("/")[-1])
                        if file_date_match:
                            file_date_found = True
                            file_date = parse_file_date(file_date_match.group(0))
                            try:
                                num_rows_before_load_job = bq_client.get_num_rows(self.dataset_id, self.table_id)
                                load_data_query = create_load_data_query(self.project_id, self.dataset_id, self.table_id, schema, [blob], field_delimiter=self.field_delimiter)
                                logger.info(f"Executing load query: {load_data_query}")
                                load_job = bq_client.query(load_data_query)
                                logger.info(f"Job Id: {load_job.job_id}")
                                load_job.result()
                                num_rows_after_load_job = bq_client.get_num_rows(self.dataset_id, self.table_id)
                                if (num_rows_after_load_job - num_rows_before_load_job) <= 0:
                                    raise ZeroRowsLoadedException
                            except ZeroRowsLoadedException:
                                # Update extraction_sync_dt to file date
                                logger.info(f"Found 0 rows in the file. Hence updating the extraction_sync_dt in mapping")
                                update_extraction_sync_dt_job = bq_client.query(f"""
                                                UPDATE {self.project_id}.{self.dataset_id}.{Variable.get('mapping_table')}
                                                SET extraction_sync_dt='{file_date}'
                                                WHERE view='{self.table_id}'
                                """)
                                logger.info(f"Running update_extraction_sync_dt_job\n{update_extraction_sync_dt_job}\nJob Id: {update_extraction_sync_dt_job.job_id}")
                                update_extraction_sync_dt_job.result()
                                logger.info(f"Successfully updated the extraction_sync_dt to {file_date}")
                            except Exception as e:
                                errors = load_job.errors
                                logger.error(f"{load_job.errors}", exc_info=True)
                                logger.error(f"""Error occured while loading {blob} to BigQuery.
                                    Refer Job {load_job.job_id} for more details""")
                                raise e
                            else:
                                logger.info(f"Successfully loaded data into {self.project_id}.{self.dataset_id}.{self.table_id}")
                                try:
                                    max_timestamp_query = f"""SELECT MAX(SYNCSTARTDATETIME) as syncstartdatetime FROM {self.project_id}.{self.dataset_id}.{self.table_id}"""
                                    logger.info(f"max timestamp query: {max_timestamp_query}")
                                    max_timestamp_job = bq_client.query(max_timestamp_query)
                                    res = max_timestamp_job.result()
                                    df = res.to_dataframe()
                                    max_timestamp = str(df.syncstartdatetime[0])
                                    update_file_date_query = f"""UPDATE {self.project_id}.{self.dataset_id}.{self.table_id} 
                                    SET SYNCSTARTDATETIME='{file_date}'  
                                    WHERE SYNCSTARTDATETIME IN ('{max_timestamp}')"""
                                    update_job = bq_client.query(update_file_date_query)
                                    logger.info(f"Running update_file_date_query for {blob}\n{update_file_date_query}\nJob Id: {update_job.job_id}")
                                    update_job.result()
                                except Exception as e:
                                    logger.error(f"{update_job.errors}", exc_info=True)
                                    logger.error(f"""Error occured while updating SYNCSTARTDATETIME.
                                        Refer Job {update_job.job_id} for more details""")
                                    raise e
                                else:
                                    logger.info(f"Successfully updated SYNCSTARTDATETIME for {blob}")
                        else:
                            file_date_found = False
                            logger.warn(f"File date match not found. Hence terminating the loading of files into {self.table_id}")
                            break
                    if not file_date_found:
                        logger.warn(f"File date match not found for one of the files. Hence terminating the loading of files into {self.table_id}")
                        break
            
            else:
                logger.warn(f"Given uris {self.uris} doesnt exist")

        except Exception  as e:
            logger.error("Exception occured", exc_info=True)
            raise AirflowException(e)