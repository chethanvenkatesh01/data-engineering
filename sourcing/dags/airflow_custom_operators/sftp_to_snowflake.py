from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.utils.decorators import apply_defaults
from airflow_options.utils import TryExcept
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
import re

class SftpToSnowflakePipeline(BeamRunJavaPipelineOperator):

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
            header: bool,
            pull_type: str = "full",
            autoscaling_algorithm: str = "THROUGHPUT_BASED",
            default_file_encoding: str = "None",
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
        self.pull_type = pull_type
        self.autoscaling_algorithm = autoscaling_algorithm
        self.default_file_encoding = default_file_encoding
        self.tenant = tenant
        self.pipeline = pipeline
        self.header = header
        self.normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', self.view)
        self.job_name = f"sftp-to-snowflake-{self.normalized_view_name.replace('_', '-').lower()}-{tenant.lower().replace('_', '-')}-{pipeline}"

        super().__init__(
            task_id=task_id,
            runner=runner,
            jar=f"gs://dataflow-uber-jars/{jar}",
            job_class="com.impact.SftpToSnowflakePipeline",
            dataflow_config={
                "poll_sleep": 10,
                "job_name": self.job_name,
                "location": self.region
            },
            max_active_tis_per_dag=1,
            pipeline_options={},
            *args, **kwargs
        )


    @staticmethod
    def _get_folder_date_pattern_from_config(config: dict):
        """Extract folder date pattern from config"""
        import re
        file_pattern = config.get("file_pattern", "yyyy-MM-dd/{view}_\d+.*.(csv|dat|txt|csv.gz|dat.gz|txt.gz|gz|parquet)")
        date_match = re.search("(yyyy-MM-dd|yyyyMMdd|yyMMdd)", file_pattern)
        return date_match.group(0) if date_match else "yyyy-MM-dd"

    @TryExcept
    def execute(self, context):
        # Fetch XCom data
        xcom_data_source_config = self.xcom_pull(context=context, dag_id=context.get('dag').dag_id, task_ids=self.xcom_task_id, key='return_value')

        # Extract pipeline-specific variables from Airflow
        self.file_prefix = Variable.get(f"{self.view}_file_prefix", default_var=f"{self.view}")
        self.file_suffix = Variable.get(f"{self.view}_file_suffix", default_var="(csv|dat|txt|csv.gz|dat.gz|txt.gz|gz|parquet)")
        self.field_delimiter = Variable.get(f"{self.view}_param_field_delimiter", default_var="|")
        self.replace_table = str(Variable.get(f"{self.view}_param_replace", default_var="false")).lower()
        self.last_processed_date = Variable.get(f"{self.view}_param_extraction_sync_dt")
        self.replace_special_chars = str(Variable.get(f"{self.view}_param_replace_special_characters", default_var="false")).lower()
        self.directories = eval(Variable.get(f"{self.view}_dirs_with_trigger_file", default_var="[]"))
        self.folder_date_pattern = Variable.get(f"{self.view}_folder_date_pattern", default_var=self._get_folder_date_pattern_from_config(xcom_data_source_config))

        self.use_standard_csv_parser = "true"
        miscellaneous_attributes_json = eval(Variable.get(f"{self.view}_param_miscellaneous_attributes", default_var="None"))
        if miscellaneous_attributes_json:
            self.use_standard_csv_parser = miscellaneous_attributes_json.get("use_standard_csv_parser", "true")

        # Check if XCom data exists
        if not xcom_data_source_config:
            print("xcom data source config: ", xcom_data_source_config)
            raise AirflowFailException(f"No XCom data found for task ID: {self.xcom_task_id}")

        self.dataflow_config = {
            "poll_sleep": 10,
            "job_name": self.job_name,
            "location": self.region,
            "project_id": self.project
        }

        self.pipeline_options.update({
            "project": self.project,
            "serviceAccount": self.service_account,
            "region": self.region,
            "tempLocation": self.temp_location,
            "stagingLocation": self.staging_location,
            "numWorkers": self.num_workers,
            "maxNumWorkers": self.max_num_workers,
            "workerMachineType": self.worker_machine_type,
            "subnetwork": self.subnetwork,
            "usePublicIps": self.use_public_ips,
            "sftpHost": xcom_data_source_config.get("server") or xcom_data_source_config.get("host"),
            "sftpUsername": xcom_data_source_config.get("user"),
            "sftpPassword": xcom_data_source_config.get("password"),
            "snowflakeTable": self.snowflake_table,
            "SnowflakeSecretName": self.SnowflakeSecretName,  
            "rootDirectory": xcom_data_source_config.get("path"),
            "filePrefix": self.file_prefix,
            "fileSuffix": self.file_suffix,
            "fieldDelimiter": self.field_delimiter,
            "replaceTable": self.replace_table,
            "header": self.header,
            "folderDatePattern": self.folder_date_pattern,
            "pullType": self.pull_type,
            "fileEncoding": self.default_file_encoding
        })

        return super().execute(context)
