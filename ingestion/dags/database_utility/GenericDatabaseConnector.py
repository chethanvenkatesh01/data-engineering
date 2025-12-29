import pandas as pd
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from airflow.models import Variable
from urllib.parse import quote_plus
from google.cloud.exceptions import NotFound
from enum import Enum
from airflow.utils.log.logging_mixin import LoggingMixin
import time
import threading
_event_listener_lock = threading.RLock()

log = LoggingMixin().log


class DatabaseConnector:
    def __init__(self, db_type, username, password, host, port, database):
        self.db_type = db_type.lower()
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = self._create_engine()
        self.session_maker = sessionmaker(self.engine, expire_on_commit=False)

    def _create_engine(self):
        if self.db_type == "postgresql":
            conn_str = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == "mysql":
            conn_str = f"mysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == "oracle":
            conn_str = f"oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == "mssql":
            conn_str = f"mssql+pyodbc://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
        return create_engine(conn_str)

    def execute_query(self, query, return_df: bool, params=None):
        """
        Execute a SQL query synchronously and return results as a DataFrame.
        :param query: SQL query to execute.
        :param params: Optional parameters for the query.
        :return: DataFrame with query results.
        """
        with self.session_maker() as session:
            try:
                result = session.execute(text(query), params)

                if return_df:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    session.commit() 
                    return df
                else:
                    session.commit() 
                    return result.rowcount
                    
            except SQLAlchemyError as e:
                log.info(f"Error executing query: {e}")
                return None

    def read_table(self, table_name):
        """
        Read a table from the database synchronously and return it as a DataFrame.
        :param table_name: Name of the table to read.
        :return: DataFrame with table data.
        """
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query, True)

    def write_table(self, df, table_name, if_exists='replace'):
        """
        Write a DataFrame to a database table synchronously.
        :param df: DataFrame to write.
        :param table_name: Name of the target table.
        :param if_exists: What to do if the table exists ('replace', 'append', 'fail').
        """
        try:
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
            log.info(f"Data written to table {table_name} successfully.")
        except SQLAlchemyError as e:
            log.info(f"Error writing to table {table_name}: {e}")


class WarehouseConnector:
    def __init__(self, warehouse_type: str, warehouse_kwargs: dict):
        """
        :param warehouse_type: The type of the database ('bigquery', 'snowflake').
        :param warehouse_kwargs: Dictionary containing connection details specific to the warehouse type.
        """
        self.warehouse_type = warehouse_type.upper()
        self.warehouse_kwargs = eval(warehouse_kwargs)
        self._set_kwargs()
        self.engine = self._create_engine()
        self.session_maker = sessionmaker(self.engine, expire_on_commit=False)
        
        with _event_listener_lock:
            # Check if event listeners are already registered
            if not getattr(Engine, '_event_listeners_registered', False):
                if self.warehouse_type == "BIGQUERY":
                    event.listen(Engine, 'after_cursor_execute', self._log_bigquery_job_id)
                elif self.warehouse_type == "SNOWFLAKE":
                    event.listen(Engine, 'after_cursor_execute', self._log_snowflake_query_id)
                # Mark that listeners have been registered
                Engine._event_listeners_registered = True
                
    def _set_kwargs(self):
        if self.warehouse_type == "SNOWFLAKE":
            self.user = self.warehouse_kwargs.get("user", "")
            self.password = quote_plus(self.warehouse_kwargs.get("password", ""))
            self.host = self.warehouse_kwargs.get("host", "")
            self.database = self.warehouse_kwargs.get("database", "")
            self.schema = self.warehouse_kwargs.get("schema", "")
            self.warehouse = self.warehouse_kwargs.get("warehouse", "")
            self.account = self.warehouse_kwargs.get("account", "")
            self.role = self.warehouse_kwargs.get("role", "")

        elif self.warehouse_type == "BIGQUERY":
            self.project = self.warehouse_kwargs.get("project", "")
            self.billing_project = self.warehouse_kwargs.get("billing_project", "")
            self.dataset = self.warehouse_kwargs.get("dataset", "")
            self.reading_project = self.warehouse_kwargs.get("reading_project", "")
            self.reading_dataset = self.warehouse_kwargs.get("reading_dataset", "")
            self.bigquery_region = self.warehouse_kwargs.get("bigquery_region", "")
            self.region = self.warehouse_kwargs.get("region", "")

        else:
            raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")

    def _create_engine(self):
        # Existing _create_engine implementation...
        if self.warehouse_type == "BIGQUERY":
            conn_str = f"bigquery://{self.billing_project}?region={self.bigquery_region}"
        elif self.warehouse_type == "SNOWFLAKE":
            conn_str = f"snowflake://{self.user}:{self.password}@{self.account}/{self.database}/{self.schema}?warehouse={self.warehouse}&role={self.role}"
            log.info(conn_str)
        else:
            raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")
        return create_engine(conn_str)

    def _log_bigquery_job_id(self, conn, cursor, statement, parameters, context, executemany):
        """Event listener to capture and log BigQuery job ID"""
        try:
            if hasattr(cursor, '_query_job'):
                job_id = cursor._query_job.job_id
                if not hasattr(self, '_last_logged_job_id') or self._last_logged_job_id != job_id:
                    self._last_logged_job_id = job_id
                    log.info(f"BigQuery Job ID: {job_id}")
                return job_id
            return None
        except Exception as e:
            if 'psycopg2' not in str(e):
                log.warning(f"Unable to capture BigQuery job ID: {str(e)}")
            return None

    def _log_snowflake_query_id(self, conn, cursor, statement, parameters, context, executemany):
        """Event listener to capture and log Snowflake query ID"""
        try:
            if hasattr(cursor, 'sfqid'):
                query_id = cursor.sfqid
                if not hasattr(self, '_last_logged_query_id') or self._last_logged_query_id != query_id:
                    self._last_logged_query_id = query_id
                    log.info(f"Snowflake Query ID: {query_id}")
                return query_id
            return None
        except Exception as e:
            log.warning(f"Unable to capture Snowflake query ID: {str(e)}")
            return None

    def execute_query(self, query: str, return_df: bool, params=None):
        """
        Execute a SQL query synchronously and return results as a DataFrame.
        :param query: SQL query to execute.
        :param return_df: Whether to return results as a DataFrame.
        :param params: Optional parameters for the query.
        :return: DataFrame with query results or row count.
        """
        with self.session_maker() as session:
            try:
                start_time = time.time()
                log.info(f"Executing query: {query}")
                result = session.execute(text(query), params)
                end_time = time.time()
                execution_time = end_time - start_time
                log.info(f"Query execution time: {execution_time:.2f} seconds")

                if return_df:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    session.commit()
                    log.info(f"Query returned {len(df)} rows")
                    return df
                else:
                    affected_rows = result.rowcount
                    session.commit()
                    log.info(f"Query affected {affected_rows} rows")
                    return affected_rows
                    
            except SQLAlchemyError as e:
                error_message = str(e).lower()
                if "table not found" in error_message or "does not exist" in error_message or "already exists" in error_message:
                    log.error(f"Table not found: {e}")
                    return None
                else:
                    log.error(f"Query Failed: {e}")
                    raise ValueError(f"Query Failed: {e}")

    def read_table(self, table_name):
        """
        Read a table from the database synchronously and return it as a DataFrame.
        :param table_name: Name of the table to read.
        :return: DataFrame with table data.
        """
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query, True)

    def write_table(self, df, table_name, if_exists='replace'):
        """
        Write a DataFrame to a database table synchronously.
        :param df: DataFrame to write.
        :param table_name: Name of the target table.
        :param if_exists: What to do if the table exists ('replace', 'append', 'fail').
        """
        try:
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
            log.info(f"Data written to table {table_name} successfully.")
        except SQLAlchemyError as e:
            log.info(f"Error writing to table {table_name}: {e}")

    def table_exists(self, table_name: str, raise_error: bool):
        """
        Check if a table exists in the warehouse.
        :param table_name: Name of the table to check.
        :return: Boolean indicating whether the table exists.
        """
        query = None
        if self.warehouse_type == "BIGQUERY":
            query = f"""
                SELECT 1
                FROM `{self.project}.{self.dataset}.INFORMATION_SCHEMA.TABLES`
                WHERE table_name = '{table_name}'
            """
        elif self.warehouse_type == "SNOWFLAKE":
            query = f"""
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = '{table_name.upper()}'
                AND table_schema = '{self.schema.upper()}'
            """
        else:
            raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")

        if not query:
            return False
        

        result = self.execute_query(query, True)
        if not raise_error:
            return True if not result.empty else False
        else:
            if result.empty:
                raise NotFound(f"Table not found {table_name} !!")
            return True
    
    def _get_complete_table_name(self, table_name: str, reading_project: bool):
        if self.warehouse_type == "BIGQUERY":
            return f"`{self.project}.{self.dataset}.{table_name}`" if not reading_project else f"`{self.reading_project}.{self.reading_dataset}.{table_name}`"
        elif self.warehouse_type == "SNOWFLAKE":
            return f"{self.database}.{self.schema}.{table_name}"
        else:
            raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")

    def _get_table_columns(self, table_name: str):
        """
        Get all column names for a given table from the information schema.
        :param table_name: Name of the table.
        :return: List of column names.
        """
        query = None
        if self.warehouse_type == "BIGQUERY":
            query = f"""
                SELECT lower(column_name) as column_name
                FROM `{self.project}.{self.dataset}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
            """
        elif self.warehouse_type == "SNOWFLAKE":
            query = f"""
                SELECT lower(column_name) as column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name.upper()}'
                AND table_schema = '{self.schema.upper()}'
            """
        else:
            raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")

        result = self.execute_query(query, True)
        if result is not None and not result.empty:
            return result['column_name'].tolist()
        else:
            return []

    def _get_table_columns_dtypes(self, table_name: str):
        """
        Get all column names and data types for a given table from the information schema.
        :param table_name: Name of the table.
        :return: DataFrame with columns `column_name` and `data_type`.
        """
        query = None
        if self.warehouse_type == "BIGQUERY":
            query = f"""
                SELECT lower(column_name) as column_name, data_type
                FROM `{self.project}.{self.dataset}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
            """
        elif self.warehouse_type == "SNOWFLAKE":
            query = f"""
                SELECT lower(column_name) as column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name.upper()}'
                AND table_schema = '{self.schema.upper()}'
            """
        else:
            raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")

        result = self.execute_query(query, True)
        if result is not None and not result.empty:
            return result[['column_name', 'data_type']]
        else:
            return pd.DataFrame(columns=['column_name', 'data_type'])
    
    def _get_row_count(self, table_name: str) -> int:
        """
        Get the row count for a specified table.
        
        :param table_name: Name of the table to get the row count for.
        :return: The row count of the table as an integer.
        """
        if self.warehouse_type == "BIGQUERY":
            query = f"""
                SELECT row_count
                FROM `{self.project}.{self.dataset}.__TABLES__`
                WHERE table_id = '{table_name}'
            """
        elif self.warehouse_type == "SNOWFLAKE":
            query = f"""
                SELECT row_count
                FROM information_schema.tables
                WHERE table_name = '{table_name.upper()}'
                AND table_schema = '{self.schema.upper()}'
            """
        else:
            raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")
        
        result = self.execute_query(query, True)
        if result is not None and not result.empty:
            return int(result.iloc[0]['row_count'])
        else:
            raise ValueError(f"Could not retrieve row count for table: {table_name}")
    
    def _get_table_list(self, keyword: str) -> list:
        """
        Get a list of tables that match the specified keyword.
        
        :param keyword: The keyword to search for in table names.
        :return: A list of table names that match the keyword.
        """
        if self.warehouse_type == "BIGQUERY":
            query = f"""
                SELECT table_id
                FROM `{self.project}.{self.dataset}.__TABLES__`
                WHERE table_id LIKE '{keyword}'
            """
        elif self.warehouse_type == "SNOWFLAKE":
            query = f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name LIKE '{keyword.upper()}'
                AND table_schema = '{self.schema.upper()}'
            """
        else:
            raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")
        
        result = self.execute_query(query, True)
        if result is not None and not result.empty:
            return result.iloc[:, 0].tolist()  # Extract the first column (table names) as a list
        else:
            raise ValueError(f"No tables found with the keyword: {keyword}")
    
    def copy_table(self, source_table_name: str, destination_table_name: str = None):
        """
        Create a copy of a table with an optional destination table name.
        If no destination name is provided, appends current date to the source table name.
        
        :param source_table_name: Name of the source table to copy
        :param destination_table_name: Optional name for the destination table
        :return: Boolean indicating success or failure of the copy operation
        """
        from datetime import datetime
        
        if destination_table_name is None:
            date_suffix = str(datetime.now().date()).replace("-", "_")
            destination_table_name = f"{source_table_name}__{date_suffix}"
        
        try:
            # Verify source table exists
            if not self.table_exists(source_table_name, False):
                log.info(f"Source table {source_table_name} does not exist.")
                return False
            
            # Get full table names for source and destination
            source_full_table_name = self._get_complete_table_name(source_table_name, False)
            dest_full_table_name = self._get_complete_table_name(destination_table_name, False)
            
            # Construct copy query based on warehouse type
            if self.warehouse_type == "BIGQUERY":
                copy_query = f"""
                CREATE TABLE {dest_full_table_name}
                AS SELECT * FROM {source_full_table_name}
                """
            elif self.warehouse_type == "SNOWFLAKE":
                copy_query = f"""
                CREATE TABLE {dest_full_table_name}
                CLONE {source_full_table_name}
                """
            else:
                raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")
            
            # Execute the copy query
            self.execute_query(copy_query, False)
            
            log.info(f"Successfully copied table {source_table_name} to {destination_table_name}")
            return True
        
        except Exception as e:
            log.info(f"Failed to copy table {source_table_name}. Exception: {e}")
            return False


    def append_to_table_using_df(self, df, table_name: str):
        """
        Append data to an existing table with schema validation and dynamic handling for BigQuery and Snowflake.

        :param df: DataFrame to append.
        :param table_name: Name of the target table.
        :return: None
        """
        if not self.table_exists(table_name, raise_error=False):
            raise ValueError(f"Table {table_name} does not exist in the warehouse.")

        existing_schema = self._get_table_columns_dtypes(table_name)
        if existing_schema.empty:
            raise ValueError(f"Could not fetch schema for table {table_name}.")

        table_columns = existing_schema['column_name'].tolist()
        if not all(col in df.columns for col in table_columns):
            missing_cols = set(table_columns) - set(df.columns)
            raise ValueError(f"DataFrame is missing required columns: {missing_cols}")

        df = df[table_columns]
        try:
            if self.warehouse_type == "BIGQUERY":
                full_table_name = self._get_complete_table_name(table_name, False)
                project_dataset = ".".join(full_table_name.strip("`").split(".")[:2])  
                df.to_sql(table_name, con=self.engine, schema=project_dataset, if_exists='append', method='multi', index=False)
                log.info(f"Data appended successfully to BigQuery table {full_table_name}.")

            elif self.warehouse_type == "SNOWFLAKE":
                full_table_name = self._get_complete_table_name(table_name, False)
                database_schema = ".".join(full_table_name.split(".")[:2])  
                df.to_sql(table_name, con=self.engine, schema=database_schema, if_exists='append', method='multi', index=False)
                log.info(f"Data appended successfully to BigQuery table {full_table_name}.")

            else:
                raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")

        except Exception as e:
            raise RuntimeError(f"Failed to append data to table {table_name}: {e}")
