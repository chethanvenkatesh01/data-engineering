from typing import List, Union

from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from dataflow_options.utils import Logger
log = Logger(__name__)

class BigQuery:
    def __init__(self, project_id:str=None, location: str = None) -> None:
        from google.cloud import bigquery
        import google.auth
        default_creds, default_project = google.auth.default()
        self.project_id = project_id if project_id else default_project
        self.__client = bigquery.Client(project=self.project_id, location=location)
    
    def get_table_as_dataframe(self, dataset_id:str=None, table_id:str=None, sql:str=None):
        if table_id:
            assert dataset_id is not None, "dataset_id shouldn't be None when table_id is provided"
            table = self.__client.get_table(f"{self.project_id}.{dataset_id}.{table_id}")
            df = self.__client.list_rows(table).to_dataframe()
            return df
        elif sql:
            df = self.__client.query(sql).to_dataframe()
            return df
        else:
            raise Exception(f"Either of these [table_id, sql] arguments should be provided")

    def get_table_schema_as_json(self, dataset_id:str, table_id:str):
        df = self.get_table_as_dataframe(sql=f"""
        SELECT 
            column_name as name, 
            data_type as type,
            CASE
                WHEN is_nullable='YES' THEN 'NULLABLE'
                ELSE 'REQUIRED'
            END AS mode
        FROM {dataset_id}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name='{table_id}'
        ORDER BY ordinal_position""")
        return df.to_dict('records')
    
    def generate_load_query(self, dataset_id:str, table_id:str, uris:Union[str, List[str]], file_format:str='CSV', 
                            field_delimiter:str=',', skip_leading_rows:int=0,
                            columns_to_exclude:List[str]=[]) -> str:
        from typing import List, Union, Dict
        schema:List[Dict] = self.get_table_schema_as_json(dataset_id, table_id)
        load_query_schema = ""
        for field in schema:
            if field['name'].upper() in columns_to_exclude:
                continue
            load_query_schema += f"{field['name']} {field['type']} {'NOT NULL' if field['mode']=='REQUIRED' else ''},\n"
        load_query_schema = load_query_schema.strip(',\n') + "\n"
        if isinstance(uris, str):
            uris = [uris]
        
        assert isinstance(uris, list), "The argument 'uris' is not of list type. Please provide it as str or List[str]"
        
        if file_format.upper() == 'PARQUET':
            query = f"""LOAD DATA INTO {self.project_id}.{dataset_id}.{table_id}
            (
            {load_query_schema}
            )
            FROM FILES
            (
                uris = {uris},
                format = '{file_format}'
            )
            """
        elif file_format.upper() == 'CSV':
            query = f"""LOAD DATA INTO {self.project_id}.{dataset_id}.{table_id}
            (
            {load_query_schema}
            )
            FROM FILES
            (
                uris = {uris},
                format = '{file_format}',
                field_delimiter = '{field_delimiter}',
                skip_leading_rows = {skip_leading_rows},
                allow_quoted_newlines = true,
                quoting = 'AUTO'
            )
            """
        else:
            raise Exception(f"The file format {file_format} is not supported")

        return query

    def executeQuery(self, sql:str) -> Union[RowIterator, _EmptyRowIterator]:
        log.info(f"Started executing query:\n{sql}")
        query_job = self.__client.query(sql)
        row_iterator = query_job.result()
        log.info(f"The query completed")
        return row_iterator, query_job.job_id
    
    def query(self, sql:str):
        return self.__client.query(sql)
    
    def get_num_rows(self, dataset_id:str, table_id:str):
        return self.__client.get_table(f"{self.project_id}.{dataset_id}.{table_id}").num_rows

class AsyncGBQClient:
    """
    Asynchronous client for Bigquery
    """

    def __init__(self):
        import asyncio
        self.loop = asyncio.get_event_loop()

    async def run_query(self,query):
        import pandas as pd
        df = pd.read_gbq(query)
        return df
    
    def run_queries(self, queries):

        import asyncio
        tasks = [
            self.loop.create_task(
                self.run_query(query))
            for query in queries]
        
        groups = asyncio.gather(*tasks)
        
        res = self.loop.run_until_complete(groups)
        return res