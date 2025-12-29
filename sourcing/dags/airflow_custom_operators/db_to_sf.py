from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.utils.decorators import apply_defaults
from airflow_options.utils import TryExcept
from airflow.exceptions import AirflowFailException
import re
from airflow.models import Variable

class DbToSfPipeline(BeamRunJavaPipelineOperator):

    @apply_defaults
    def __init__(
            self,
            xcom_task_id: str,
            view: str,
            task_id: str,
            runner: str,
            jar: str,
            tenant: str,
            pipeline: str,
            project: str,
            billing_project: str,
            service_account: str,
            region: str,
            temp_location: str,
            staging_location: str,
            num_workers: int,
            max_num_workers: int,
            worker_machine_type: str,
            subnetwork: str,
            use_public_ips: bool,
            snowflake_table: str,
            SnowflakeSecretName: str, 
            query_partitioning_threshold: int = 1000000000,
            max_partition_size: int = 1000000,
            autoscaling_algorithm: str = "THROUGHPUT_BASED",
            *args, **kwargs):
        
        self.xcom_task_id = xcom_task_id
        self.view = view
        self.task_id = task_id
        self.project = project
        self.billing_project = billing_project
        self.service_account = service_account
        self.region = region
        self.temp_location = temp_location
        self.staging_location = staging_location
        self.num_workers = num_workers
        self.max_num_workers = max_num_workers
        self.worker_machine_type = worker_machine_type
        self.subnetwork = subnetwork
        self.use_public_ips = use_public_ips
        self.snowflake_table = snowflake_table
        self.SnowflakeSecretName = SnowflakeSecretName
        self.query_partitioning_threshold = query_partitioning_threshold
        self.max_partition_size = max_partition_size
        self.autoscaling_algorithm = autoscaling_algorithm
        self.tenant = tenant
        self.pipeline = pipeline
        self._dataflow_job_name = f"db-to-sf-{view.replace('_', '-').lower()}-{tenant.lower().replace('_', '-')}-{pipeline}"
        self.dataflow_config = {
            "poll_sleep": 10,
            "job_name": self._dataflow_job_name,
            "location": self.region
        }
        super().__init__(
            task_id=task_id,
            runner=runner,
            jar=f"gs://dataflow-uber-jars/{jar}",
            job_class="com.impact.DbToSfPipeline",
            dataflow_config=self.dataflow_config,
            max_active_tis_per_dag=1,
            pipeline_options={},
            *args, **kwargs
        )

    @TryExcept
    def execute(self, context):
        # Fetch XCom data
        xcom_data_source_config = self.xcom_pull(
            context=context, dag_id=context.get('dag').dag_id, task_ids=self.xcom_task_id, key='return_value'
        )

        # Check if XCom data exists
        if not xcom_data_source_config:
            print("xcom data source config: ", xcom_data_source_config)
            raise AirflowFailException(f"No XCom data found for task ID: {self.xcom_task_id}")

        # Extract pipeline variables from Airflow
        self.audit_column = Variable.get(f"{self.view}_param_filter", default_var="SYNCSTARTDATETIME")
        self.replace_table = str(Variable.get(f"{self.view}_param_replace", default_var="false")).lower()
        self.replace_special_chars = str(Variable.get(f"{self.view}_param_replace_special_characters", default_var="false")).lower()
        self.pull_type = Variable.get(f"{self.view}_param_pull_type", default_var="incremental")
        
        # Database connection variables
        self.db_secret = Variable.get(f"{self.view}_param_source_config", default_var="null")
        self.incremental_column = Variable.get(f"{self.view}_param_filter", default_var="null")
        self.incremental_column_value = Variable.get(f"{self.view}_param_extraction_sync_dt", default_var="null")
        
        # Ensure timestamp format consistency
        if re.search(r"\.\d{6}", self.incremental_column_value):
            self.incremental_column_value = self.incremental_column_value[:-3]

        self.db_type = Variable.get(f"{self.view}_param_connector")
        self.db_schema = xcom_data_source_config.get("schema")

        # Pipeline options for Snowflake ingestion
        self.pipeline_options.update({
            "project": self.project,
            "serviceAccount": self.service_account,
            "region": self.region,
            "tempLocation": self.temp_location,
            "stagingLocation": self.staging_location,
            "autoscalingAlgorithm": self.autoscaling_algorithm,
            "numWorkers": self.num_workers,
            "maxNumWorkers": self.max_num_workers,
            "workerMachineType": self.worker_machine_type,
            "subnetwork": self.subnetwork,
            "usePublicIps": self.use_public_ips,
            "dbSecretName": self.db_secret,
            "dbType": self.db_type,
            "dbSchemaName": self.db_schema,
            "dbTable": self.view,
            "pullType": self.pull_type,
            "incrementalColumn": self.incremental_column,
            "incrementalColumnValue": self.incremental_column_value,
            "SnowflakeSecretName": self.SnowflakeSecretName,  
            "snowflakeTable": self.snowflake_table,
            "replaceTable": self.replace_table,
            "queryPartitioningThreshold": self.query_partitioning_threshold,
            "maxPartitionSize": self.max_partition_size,
            "auditColumn": self.audit_column,
            "replaceSpecialChars": self.replace_special_chars
        })

        return super().execute(context)
