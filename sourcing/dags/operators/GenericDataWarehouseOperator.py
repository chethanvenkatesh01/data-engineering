from airflow.models.baseoperator import BaseOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.decorators import apply_defaults
import logging

log = logging.getLogger(__name__)


class BigQueryOperatorWrapper(BaseOperator):
    def __init__(self, extras, conn_id, sql, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.extras = extras
        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context):
        operator = BigQueryOperator(
            task_id=self.task_id,
            sql=self.sql,
            gcp_conn_id=self.conn_id,
            use_legacy_sql=False,
        )
        operator.execute(context)


class SQLExecuteOperatorWrapper(BaseOperator):
    def __init__(self, extras, conn_id, sql, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.extras = extras
        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context):
        log.info("inside SQLExecuteOperatorWrapper")
        log.info(f"conn_id : {self.conn_id}")
        operator = SQLExecuteQueryOperator(
            task_id=self.task_id,
            sql=self.sql,
            conn_id=self.conn_id,
            split_statements=True,
            return_last=False,
        )
        operator.execute(context)


def get_data_warehouse_operator(warehouse, sql, conn_id, *args, **kwargs):

    log.info(f"warehouse: ", warehouse)
    log.info(f"conn id : {conn_id}")

    if warehouse == "BIGQUERY":
        return BigQueryOperatorWrapper(
            extras=kwargs.get("extras"), conn_id=conn_id, sql=sql, *args, **kwargs
        )
    else:
        return SQLExecuteOperatorWrapper(
            extras=kwargs.get("extras"), conn_id=conn_id, sql=sql, *args, **kwargs
        )


class GenericDataWarehouseOperator(BaseOperator):
    template_fields = ("sql", "warehouse")

    @apply_defaults
    def __init__(self, warehouse, sql, conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.warehouse = warehouse
        self.sql = sql
        self.conn_id = conn_id
        self.args = args
        self.kwargs = kwargs

    def execute(self, context):
        print(self.sql)
        operator = get_data_warehouse_operator(
            self.warehouse, self.sql, self.conn_id, *self.args, **self.kwargs
        )
        operator.execute(context)
