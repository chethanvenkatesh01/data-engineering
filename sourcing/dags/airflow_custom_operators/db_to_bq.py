from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
)
from airflow.utils.decorators import apply_defaults
from airflow_options.utils import TryExcept
from airflow.exceptions import AirflowFailException
import os

class DbToBQPipeline(BeamRunJavaPipelineOperator):

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
            billing_project:str,
            service_account: str,
            region: str,
            bigquery_region: str,
            temp_location: str,
            staging_location: str,
            num_workers: int,
            max_num_workers: int,
            worker_machine_type: str,
            subnetwork: str,
            use_public_ips: bool,
            bigquery_dataset: str,
            bigquery_table: str,
            db_table: str,
            autoscaling_algorithm: str = "THROUGHPUT_BASED",
            query_partitioning_threshold: int = 1000000000,
            *args, **kwargs):
        self.xcom_task_id = xcom_task_id
        self.view = view
        self.task_id = task_id
        self.project = project
        self.billing_project = billing_project
        self.service_account = service_account
        self.region = region
        self.bigquery_region = bigquery_region
        self.temp_location = temp_location
        self.staging_location = staging_location
        self.num_workers = num_workers
        self.max_num_workers = max_num_workers
        self.worker_machine_type = worker_machine_type
        self.subnetwork = subnetwork
        self.use_public_ips = use_public_ips
        self.db_table = db_table
        self.bigquery_dataset = bigquery_dataset
        self.bigquery_table = bigquery_table
        self.autoscaling_algorithm = autoscaling_algorithm
        self.query_partitioning_threshold = query_partitioning_threshold
        self.tenant = tenant
        self.pipeline = pipeline
        self._dataflow_job_name = f"db-to-bq-{view.replace('_', '-').lower()}-{tenant.lower().replace('_', '-')}-{pipeline}"
        self.dataflow_config = {
            "poll_sleep": 10,
            "job_name": self._dataflow_job_name,
            "location": self.region
        }
        super().__init__(
            task_id=task_id,
            runner=runner,
            jar=f"gs://dataflow-uber-jars/{jar}",
            job_class="com.impact.DbToBqPipeline",
            dataflow_config=self.dataflow_config,
            max_active_tis_per_dag=1,
            pipeline_options={},
            *args, **kwargs
        )

    @staticmethod
    def _get_folder_date_pattern_from_config(config: dict):
        import re
        file_pattern:str = config.get("file_pattern", "yyyy-MM-dd/{view}_\d+.*.(csv|dat|txt|csv.gz|dat.gz|txt.gz|parquet)")
        date_match = re.search("(yyyy-MM-dd|yyyyMMdd|yyMMdd)", file_pattern)
        if date_match:
            return date_match.group(0)
        else:
            return "yyyy-MM-dd"

    @TryExcept
    def execute(self, context):
        import re
        from airflow.models import Variable

        # Fetch XCom data
        xcom_data_source_config = self.xcom_pull(context=context, dag_id=context.get('dag').dag_id, task_ids=self.xcom_task_id, key='return_value')

        # Check if XCom data exists
        if not xcom_data_source_config:
            print("xcom data source config: ", xcom_data_source_config)
            raise AirflowFailException(f"No XCom data found for task ID: {self.xcom_task_id}")
        self.audit_column = Variable.get(f"{self.view}_param_filter", default_var="SYNCSTARTDATETIME")
        self.replace_table = Variable.get(f"{self.view}_param_replace", default_var="false")
        self.replace_special_chars = Variable.get(f"{self.view}_param_replace_special_characters", default_var="false")
        self.bq_partition_column = Variable.get(f"{self.view}_param_partition_column", default_var="None")
        if self.bq_partition_column == "None":
            self.bq_partition_column = "null"
        self.bq_clustering_columns = Variable.get(f"{self.view}_param_clustering_columns", default_var="None")
        if self.bq_clustering_columns == "None":
            self.bq_clustering_columns = "null"
        else:
            self.bq_clustering_columns = ",".join(eval(self.bq_clustering_columns))
        self.pull_type = Variable.get(f"{self.view}_param_pull_type", default_var="incremental")

        self.db_secret = Variable.get(f"{self.view}_param_source_config", default_var="null")
        self.incremental_column = Variable.get(f"{self.view}_param_filter", default_var="null")
        self.incremental_column_value = Variable.get(f"{self.view}_param_extraction_sync_dt", default_var="null")
        if re.search("\.\d{6}", self.incremental_column_value):
            self.incremental_column_value = self.incremental_column_value[:-3]
        self.db_type = Variable.get(f"{self.view}_param_connector")

        self.db_schema = xcom_data_source_config.get("schema")



        self.pipeline_options.update({
            "project": self.project,
            "bigqueryBillingProject": self.billing_project,
            "serviceAccount": self.service_account,
            "bigqueryRegion": self.bigquery_region,
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
            "dbTable": self.db_table,
            "bigqueryDataset": self.bigquery_dataset,
            "bigqueryTable": self.view,
            "pullType": self.pull_type,
            "incrementalColumn": self.incremental_column,
            "incrementalColumnValue": self.incremental_column_value,
            "replaceTable": str(self.replace_table).lower(),
            "auditColumn": self.audit_column,
            "replaceSpecialChars": str(self.replace_special_chars).lower(),
            "bqPartitionColumn": self.bq_partition_column,
            "bqClusteringColumns": self.bq_clustering_columns,
            "queryPartitioningThreshold": self.query_partitioning_threshold,
            "dbSchemaName": self.db_schema,
            "dbType": self.db_type,
        })

        return super().execute(context)