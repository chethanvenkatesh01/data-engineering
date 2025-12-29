from operators.GenericDbToWarehouseOperator import (
    DbToGbqOperator,
    DbToSnowflakeOperator,
)
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

WAREHOUSE_OPERATOR_MAPPING = {
    "BIGQUERY": DbToGbqOperator,
    "SNOWFLAKE": DbToSnowflakeOperator,
}


class DbtoWareHouseOperatorSelector(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        data_flow_job_name,
        db_type,
        db_table,
        replace_table,
        warehouse_table,
        num_workers: int,
        max_num_workers: int,
        ingestion_config: dict,
        *args,
        **kwargs
    ):

        super().__init__(*args, **kwargs)  # Pass to the parent class
        self.data_flow_job_name=data_flow_job_name
        self.db_type=db_type
        self.db_table=db_table
        self.replace_table=replace_table
        self.warehouse_table=warehouse_table
        self.num_workers=num_workers
        self.max_num_workers=max_num_workers
        self.ingestion_config=ingestion_config

        
    def execute(self, context):
        from airflow.models import Variable

        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs", default_var={})
        self.operator = self.get_operator(
            warehouse,
            self.data_flow_job_name,
            self.db_type,
            self.db_table,
            self.replace_table,
            self.warehouse_table,
            self.num_workers,
            self.max_num_workers,
            self.ingestion_config,
            warehouse_kwargs,
        )

        self.operator.execute(context)

    def get_operator(
        self,
        warehouse,
        data_flow_job_name,
        db_type,
        db_table,
        replace_table,
        warehouse_table,
        num_workers,
        max_num_workers,
        ingestion_config: dict,
        warehouse_kwargs: dict,
    ):
        return WAREHOUSE_OPERATOR_MAPPING[warehouse](
            task_id=self.task_id,
            data_flow_job_name=data_flow_job_name,
            db_type=db_type,
            db_table=db_table,
            replace_table=replace_table,
            warehouse_table=warehouse_table,
            num_workers=num_workers,
            max_num_workers=max_num_workers,
            ingestion_config=ingestion_config,
            warehouse_kwargs=warehouse_kwargs,
        )
