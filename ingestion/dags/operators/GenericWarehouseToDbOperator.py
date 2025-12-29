from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
)
from airflow.models.baseoperator import BaseOperator
from pydantic import BaseModel
from typing import Optional
from constants.constant import (
    GBQ_DB_COPY_MULTI_TABLES_JAR,
    GBQ_DB_COPY_TABLES_JAR,
    SNOWFLAKE_DB_COPY_TABLES_JAR,
    SNOWFLAKE_DB_COPY_MULTI_TABLES_JAR,
)
from abc import ABC, abstractmethod
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable



class SingleTableCopyConfig(BaseModel):
    warehouse_table: str = None
    replace_table: str = None
    db_table_schema: Optional[str] = None
    db_table: str = None
    
    def is_valid(self):
        return any(
            value is not None for value in [
                self.warehouse_table, 
                self.replace_table, 
                self.db_table_schema, 
                self.db_table
            ]
        )


class MultiTableCopyConfig(BaseModel):
    warehouse_table_to_pg_table_name_map: str = None


class TableCopyConfig(BaseModel):
    single_table_copy_config: SingleTableCopyConfig = None
    multi_table_copy_config: MultiTableCopyConfig = None


class CommonWareHouseToDbOperator(BaseOperator):
    def __init__(
        self,
        data_flow_job_name: str,
        db_schema_name: str,
        db_type: str,
        num_workers: int,
        max_num_workers: int,
        ingestion_config: dict,
        warehouse_kwargs: dict,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.operator = None
        self.data_flow_job_name = data_flow_job_name
        self.db_schema_name = db_schema_name
        self.db_type = db_type
        self.ingestion_config = ingestion_config
        self.num_workers = num_workers
        self.max_num_workers = max_num_workers
        self.warehouse_kwargs = warehouse_kwargs

    def get_common_pipeline_options(self):
        common_pipeline_options = {
            "project": Variable.get('gcp_project'),
            "serviceAccount": Variable.get('service_account_email'),
            "region": Variable.get('region'),
            "tempLocation": f"gs://{Variable.get('tempbucket')}/dataflow/temp/",
            "stagingLocation": f"gs://{Variable.get('tempbucket')}/dataflow/staging/",
            "autoscalingAlgorithm": "THROUGHPUT_BASED",
            "numWorkers": self.num_workers,
            "maxNumWorkers": self.max_num_workers,
            "workerMachineType": "n2-highmem-4",
            "subnetwork": f"{Variable.get('subnetwork')}",
            "usePublicIps": False,
            "dbSecretName": f"{Variable.get('tenant')}_{Variable.get('pipeline')}_data_ingestion",
            "dbSchemaName": self.db_schema_name,
            "dbType": self.db_type,
        }

        return common_pipeline_options

    def execute(self, context):
        self.operator.execute(context) 


class GbqToDbOperator(CommonWareHouseToDbOperator):
    def __init__(
        self,
        data_flow_job_name,
        db_schema_name,
        db_type,
        table_copy_config: TableCopyConfig,
        num_workers: int,
        max_num_workers: int,
        ingestion_config,
        warehouse_kwargs,
        *args, 
        **kwargs
    ):

        super().__init__(
            data_flow_job_name=data_flow_job_name,
            db_schema_name=db_schema_name,
            db_type=db_type,
            num_workers=num_workers,
            max_num_workers=max_num_workers,
            ingestion_config=ingestion_config,
            warehouse_kwargs=warehouse_kwargs,
            *args, 
            **kwargs
        )

        self._set_operator(table_copy_config)


    def _set_operator(self, table_copy_config: TableCopyConfig):
        pipeline_options = self.get_common_pipeline_options()
        pipeline_options.update(
            {
                "bigqueryDataset": Variable.get('dataset'),
                "bigqueryRegion": self.ingestion_config.get("bigquery_region", "US"),
            }
        )
        if table_copy_config.single_table_copy_config and table_copy_config.single_table_copy_config.is_valid():
            job_class_name = "com.impact.BqToDbPipeline"
            jar_name = GBQ_DB_COPY_TABLES_JAR
            pipeline_options.update(
                {
                    "bigqueryTable": table_copy_config.single_table_copy_config.warehouse_table,
                    "replaceTable": table_copy_config.single_table_copy_config.replace_table,
                    "dbTable": table_copy_config.single_table_copy_config.db_table,
                }
            )

            if table_copy_config.single_table_copy_config.db_table_schema:
                pipeline_options.update(
                    {
                        "dbTableSchema": table_copy_config.single_table_copy_config.db_table_schema
                    }
                )

        elif table_copy_config.multi_table_copy_config:
            job_class_name = "com.impact.BqToDbCopyMultiPipeline"
            jar_name = GBQ_DB_COPY_MULTI_TABLES_JAR
            pipeline_options.update(
                {
                    "bqTableToPgTableNameMap": table_copy_config.multi_table_copy_config.warehouse_table_to_pg_table_name_map
                }
            )

        self.operator = BeamRunJavaPipelineOperator(
            task_id=self.task_id,
            runner=self.ingestion_config.get("runner", "DataFlowRunner"),
            jar=jar_name,
            pipeline_options=pipeline_options,
            job_class=job_class_name,
            dataflow_config={
                "poll_sleep": 10,
                "job_name": self.data_flow_job_name,
                "location": Variable.get('region'),
            },
        )

    

class SnowflakeToDbOperator(CommonWareHouseToDbOperator):
    def __init__(
        self,
        data_flow_job_name: str,
        db_schema_name: str,
        db_type: str,
        num_workers: int,
        max_num_workers: int,
        table_copy_config: TableCopyConfig,
        ingestion_config: dict,
        warehouse_kwargs: dict,
        *args, 
        **kwargs
    ):
        super().__init__(
            data_flow_job_name=data_flow_job_name,
            db_schema_name=db_schema_name,
            db_type=db_type,
            num_workers=num_workers,
            max_num_workers=max_num_workers,
            ingestion_config=ingestion_config,
            warehouse_kwargs=warehouse_kwargs,
            *args, 
            **kwargs
        )

        self._set_operator(table_copy_config)

    def _set_operator(self, table_copy_config: TableCopyConfig):

        # user = warehouse_kwargs["user"]
        # password = quote_plus(warehouse_kwargs["password"])
        # host = warehouse_kwargs["host"]
        # database = warehouse_kwargs["database"]
        # schema = warehouse_kwargs["schema"]
        # warehouse = warehouse_kwargs["warehouse"]
        # storage_integration = warehouse_kwargs["storage_integration"]
        # account = warehouse_kwargs["account"]
        # staging_bucket = warehouse_kwargs["sf_bucket"]
        # role = warehouse_kwargs["role"]
        pipeline_options = self.get_common_pipeline_options()
        pipeline_options.update(
            # {
            #     "storageIntegration": storage_integration,
            #     "stagingBucket": staging_bucket,
            #     "snowflakeServerName": host,
            #     "snowflakeUserName": user,
            #     "snowflakePassword": password,
            #     "snowflakeDatabase": database,
            #     "snowflakeSchema": schema,
            #     "snowflakeWarehouse": warehouse,
            #     "snowflakeUserRole": role,
            #     "snowflakeAccount": account,
            # }
            {"snowflakeSecretName": f"{Variable.get('tenant')}_{Variable.get('pipeline')}_data_ingestion_warehouse"
            }
        )

        if table_copy_config.single_table_copy_config and table_copy_config.single_table_copy_config.is_valid():
            job_class_name = "com.impact.SfToDbPipeline"
            jar_name = SNOWFLAKE_DB_COPY_TABLES_JAR
            pipeline_options.update(
                {
                    "snowflakeTable": table_copy_config.single_table_copy_config.warehouse_table,
                    "replaceTable": table_copy_config.single_table_copy_config.replace_table,
                    "dbTableSchema": table_copy_config.single_table_copy_config.db_table_schema,
                    "dbTable": table_copy_config.single_table_copy_config.db_table,
                }
            )
        elif table_copy_config.multi_table_copy_config:
            job_class_name = "com.impact.SfToDbCopyMultiPipeline"
            jar_name = SNOWFLAKE_DB_COPY_MULTI_TABLES_JAR
            pipeline_options.update(
                {
                    "sfTableToPgTableNameMap": table_copy_config.multi_table_copy_config.warehouse_table_to_pg_table_name_map
                }
            )
        self.operator = BeamRunJavaPipelineOperator(
            task_id=self.task_id,
            runner=self.ingestion_config.get("runner", "DataFlowRunner"),
            jar=jar_name,
            pipeline_options=pipeline_options,
            job_class="com.impact.SfToDbPipeline",
            dataflow_config={
                "poll_sleep": 10,
                "job_name": self.data_flow_job_name,
                "location":  Variable.get('region'),
            },
        )

    