from operators.GenericWarehouseToDbOperator import (
    GbqToDbOperator,
    SnowflakeToDbOperator,
    SingleTableCopyConfig,
    MultiTableCopyConfig,
    TableCopyConfig,
)
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from pydantic import BaseModel


WAREHOUSE_OPERATOR_MAPPING = {
    "BIGQUERY": GbqToDbOperator,
    "SNOWFLAKE": SnowflakeToDbOperator,
}


class WareHouseToDbOperatorSelector(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        data_flow_job_name,
        db_type,
        db_schema_name,
        num_workers: int,
        max_num_workers: int,
        ingestion_config: dict,
        single_table_config: dict = {},
        multiple_table_copy_config: dict = {},
        *args,
        **kwargs
    ):
        """
        single_table_copy_config = {  warehouse_table:str = None,
                                    replace_table: str = None,
                                    db_table_schema: str = None,
                                    db_table: str = None
                                }
        multi_table_copy_config = { warehouse_table_to_pg_table_name_map:str = None
                                }
        """
        super().__init__(*args, **kwargs)  # Pass to the parent class
        self.data_flow_job_name = data_flow_job_name
        self.db_type = db_type
        self.db_schema_name = db_schema_name
        self.single_table_config = single_table_config
        self.multiple_table_copy_config = multiple_table_copy_config
        self.num_workers = num_workers
        self.max_num_workers = max_num_workers
        self.ingestion_config = ingestion_config

        
    def execute(self, context):
        from airflow.models import Variable

        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs", default_var={})

        single_table_copy_config = SingleTableCopyConfig(**(self.single_table_config))
        multiple_table_copy_config = MultiTableCopyConfig(**(self.multiple_table_copy_config))
        table_copy_config = TableCopyConfig(
            single_table_copy_config=single_table_copy_config,
            multi_table_copy_config=multiple_table_copy_config,
        )

        self.operator = self.get_operator(
            warehouse,
            self.data_flow_job_name,
            self.db_type,
            self.db_schema_name,
            table_copy_config,
            self.num_workers,
            self.max_num_workers,
            warehouse_kwargs,
            self.ingestion_config,
        )

        self.operator.execute(context)

    def get_operator(
        self ,
        warehouse: str,
        data_flow_job_name: str,
        db_type: str,
        db_schema_name: str,
        table_copy_config: TableCopyConfig,
        num_workers,
        max_num_workers,
        warehouse_kwargs: dict,
        ingestion_config: dict,
    ):

        return WAREHOUSE_OPERATOR_MAPPING[warehouse](
            task_id=self.task_id,
            data_flow_job_name=data_flow_job_name,
            db_schema_name=db_schema_name,
            db_type=db_type,
            table_copy_config=table_copy_config,
            num_workers=num_workers,
            max_num_workers=max_num_workers,
            ingestion_config=ingestion_config,
            warehouse_kwargs=warehouse_kwargs,
        )
