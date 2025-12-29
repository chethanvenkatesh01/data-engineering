from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
)
from constants.constant import DB_GBQ_COPY_TABLES_JAR, DB_SNOWFLAKE_TABLES_JAR
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator


class CommonDbToWarehouseOperator(BaseOperator):
    def __init__(
        self,
        data_flow_job_name,
        db_type,
        db_table,
        replace_table,
        num_workers: int,
        max_num_workers: int,
        ingestion_config: dict,
        warehouse_kwargs: dict,
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.operator = None
        self.data_flow_job_name = data_flow_job_name
        self.db_type = db_type
        self.db_table = db_table
        self.replace_table = replace_table
        self.num_workers = num_workers
        self.max_num_workers = max_num_workers
        self.ingestion_config = ingestion_config
        self.warehouse_kwargs = warehouse_kwargs

    def get_common_pipeline_options(self):
        common_pipeline_options = {
            "project":Variable.get('gcp_project'),
            "serviceAccount": Variable.get('service_account_email'),
            "region": Variable.get('region'),
            "tempLocation": f"gs://{Variable.get('tempbucket')}/dataflow/temp/",
            "stagingLocation": f"gs://{Variable.get('tempbucket')}/dataflow/staging/",
            "autoscalingAlgorithm": "THROUGHPUT_BASED",
            "numWorkers": self.num_workers,
            "maxNumWorkers": self.max_num_workers,
            "workerMachineType": "n2-highmem-4",
            "subnetwork": Variable.get('subnetwork', ''),
            "usePublicIps": False,
            "dbSecretName": f"{Variable.get('tenant')}_{Variable.get('pipeline')}_data_ingestion",
            "dbTable": self.db_table,
            "pullType": "full",
            "queryPartitioningThreshold": 1000000000,
            "dbType": self.db_type,
            "replaceTable": self.replace_table,
        }
        return common_pipeline_options

    def execute(self, context):
        self.operator.execute(context) 


class DbToGbqOperator(CommonDbToWarehouseOperator):
    def __init__(
        self,
        data_flow_job_name: str,
        db_type: str,
        db_table: str,
        replace_table: str,
        warehouse_table: str,
        num_workers: int,
        max_num_workers: int,
        ingestion_config: dict,
        warehouse_kwargs: dict,
        *args, 
        **kwargs
    ):
        # Call the parent class's constructor
        super().__init__(
            data_flow_job_name=data_flow_job_name,
            db_type=db_type,
            db_table=db_table,
            replace_table=replace_table,
            num_workers=num_workers,
            max_num_workers=max_num_workers,
            ingestion_config=ingestion_config,
            warehouse_kwargs=warehouse_kwargs,
            *args, 
            **kwargs
        )
        self._set_operator(warehouse_table)

    def _set_operator(self, warehouse_table: str):
        pipeline_options = self.get_common_pipeline_options()
        pipeline_options.update(
            {
                "bigqueryDataset": Variable.get('dataset', ''),
                "bigqueryRegion": self.ingestion_config.get("bigquery_region", "US"),
                "bigqueryTable": f"{warehouse_table}",
            }
        )

        self.operator = BeamRunJavaPipelineOperator(
            task_id=self.task_id,
            runner=self.ingestion_config.get("runner", "DataFlowRunner"),
            jar=DB_GBQ_COPY_TABLES_JAR,
            pipeline_options=pipeline_options,
            job_class="com.impact.DbToBqPipeline",
            dataflow_config={
                "poll_sleep": 10,
                "job_name": self.data_flow_job_name,
                "location": Variable.get('region'),
            },
        )

    



class DbToSnowflakeOperator(CommonDbToWarehouseOperator):
    def __init__(
        self,
        data_flow_job_name: str,
        db_type: str,
        db_table: str,
        replace_table: str,
        warehouse_table: str,
        num_workers: int,
        max_num_workers: int,
        ingestion_config: dict,
        warehouse_kwargs: dict,
        *args, 
        **kwargs
    ):
        # Call the parent class's constructor
        super().__init__(
            data_flow_job_name=data_flow_job_name,
            db_type=db_type,
            db_table=db_table,
            replace_table=replace_table,
            num_workers=num_workers,
            max_num_workers=max_num_workers,
            ingestion_config=ingestion_config,
            warehouse_kwargs=warehouse_kwargs,
            *args, 
            **kwargs
        )
        self._set_operator(warehouse_table)

    def _set_operator(self, warehouse_table):

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
            {"snowflakeSecretName": f"{Variable.get('tenant')}_{Variable.get('pipeline')}_data_ingestion_warehouse",
             "snowflakeTable": warehouse_table
            }
        )
        self.operator = BeamRunJavaPipelineOperator(
            task_id=self.task_id,
            runner=self.ingestion_config.get("runner", "DataFlowRunner"),
            jar=DB_SNOWFLAKE_TABLES_JAR,
            pipeline_options=pipeline_options,
            job_class="com.impact.DbToSfPipeline",
            dataflow_config={
                "poll_sleep": 10,
                "job_name": self.data_flow_job_name,
                "location": Variable.get('region'),
            },
        )

    