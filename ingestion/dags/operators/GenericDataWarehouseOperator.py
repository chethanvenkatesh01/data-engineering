from airflow.models.baseoperator import BaseOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.decorators import apply_defaults

class BigQueryOperatorWrapper(BaseOperator):
    def __init__(self, 
                 extras, 
                 conn_id, 
                 sql, 
                 destination_dataset_table=None, 
                 write_disposition=None, 
                 create_disposition=None, 
                 use_legacy_sql=False,
                 *args, 
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.extras = extras
        self.conn_id = conn_id
        self.sql = sql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.use_legacy_sql = use_legacy_sql

    def execute(self, context):
        # Prepare BigQuery operator arguments
        bq_args = {
            'task_id': self.task_id,
            'sql': self.sql,
            'gcp_conn_id': self.conn_id,
            'use_legacy_sql': self.use_legacy_sql
        }
        
        # Add optional BigQuery-specific parameters
        if self.destination_dataset_table:
            bq_args['destination_dataset_table'] = self.destination_dataset_table
        if self.write_disposition:
            bq_args['write_disposition'] = self.write_disposition
        if self.create_disposition:
            bq_args['create_disposition'] = self.create_disposition
        
        
        operator = BigQueryOperator(**bq_args)
        operator.execute(context)
        # if hasattr(operator, 'job_id'):
        #     context['task_instance'].xcom_push(key='job_id', value=operator.job_id)


class SQLExecuteOperatorWrapper(BaseOperator):
    def __init__(self, 
                 extras, 
                 conn_id, 
                 sql, 
                 destination_table=None, 
                 write_disposition=None, 
                 create_disposition=None, 
                 *args, 
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.extras = extras if extras else {}
        self.conn_id = conn_id
        self.sql = sql
        self.destination_table = destination_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition

    def execute(self, context):
        # Prepare SQL operator arguments
        sql_args = {
            'task_id': self.task_id,
            'sql': self.sql,
            'conn_id': self.conn_id,
            'split_statements': self.extras.get("split_lines",None) if self.extras else True,
            'return_last': False
        }
        
        # Add custom logic for destination table and dispositions
        if self.destination_table:
            # # Handle create_disposition
            # if self.create_disposition == 'CREATE_IF_NEEDED':
            #     # Assume CREATE IF NOT EXISTS is standard SQL
            #     self.sql = f"""
            #     CREATE TABLE IF NOT EXISTS {self.destination_table} AS 
            #     {self.sql}
            #     """
            # elif self.create_disposition == 'CREATE_NEVER':
            #     # Assume table already exists, just insert/update
            #     self.sql = f"""
            #     INSERT INTO {self.destination_table} 
            #     {self.sql}
            #     """
            
            # Handle write_disposition
            if self.write_disposition == 'WRITE_TRUNCATE':
                self.sql = f"""
                CREATE OR REPLACE TABLE {self.destination_table} AS 
                {self.sql}
                """
            elif self.write_disposition == 'WRITE_APPEND':
                self.sql = f"""
                INSERT INTO {self.destination_table} 
                {self.sql}
                """
            
            sql_args['sql'] = self.sql
        
        
        operator = SQLExecuteQueryOperator(**sql_args)
        operator.execute(context)
        # if hasattr(operator, 'job_id'):
        #     context['task_instance'].xcom_push(key='job_id', value=operator.job_id)


def get_data_warehouse_operator(
    warehouse, 
    sql, 
    conn_id, 
    extras=None,
    destination_dataset_table=None, 
    write_disposition=None, 
    create_disposition=None, 
    *args, 
    **kwargs
):
    if warehouse == "BIGQUERY":
        return BigQueryOperatorWrapper(
            extras=extras, 
            conn_id=conn_id, 
            sql=sql, 
            destination_dataset_table=destination_dataset_table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            *args, 
            **kwargs
        )
    else:
        return SQLExecuteOperatorWrapper(
            extras=extras, 
            conn_id=conn_id, 
            sql=sql, 
            destination_table=destination_dataset_table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            *args, 
            **kwargs
        )


class GenericDataWarehouseOperator(BaseOperator):
    template_fields = ('sql', 'warehouse', 'destination_dataset_table','write_disposition','create_disposition')
    
    @apply_defaults
    def __init__(
        self, 
        warehouse, 
        sql, 
        conn_id, 
        destination_dataset_table=None,
        write_disposition=None,
        create_disposition=None,
        extras=None,
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs) 
        self.warehouse = warehouse
        self.sql = sql
        self.conn_id = conn_id
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.extras = extras
        self.args = args  
        self.kwargs = kwargs

    def execute(self, context):
        print(f"Executing SQL: {self.sql}")
        if self.destination_dataset_table:
            from airflow import models
            from database_utility.GenericDatabaseConnector import WarehouseConnector
            from queries.query_builder import merge_query_base_masters
            warehouse_kwargs = models.Variable.get("warehouse_kwargs")
            wc = WarehouseConnector(self.warehouse, warehouse_kwargs)
            self.destination_dataset_table = wc._get_complete_table_name(self.destination_dataset_table, False)
            self.destination_dataset_table = self.destination_dataset_table.replace("`", "")

        operator = get_data_warehouse_operator(
            self.warehouse, 
            self.sql, 
            self.conn_id, 
            extras = self.extras,
            destination_dataset_table=self.destination_dataset_table,
            write_disposition=self.write_disposition,
            create_disposition=self.create_disposition,
            *self.args, 
            **self.kwargs
        )
        operator.execute(context)