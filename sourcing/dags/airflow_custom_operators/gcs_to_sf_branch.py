import re
from typing import Optional, Callable
from airflow.models import BaseOperator
from contextlib import ExitStack
from airflow.providers.apache.beam.operators.beam import BeamDataflowMixin
from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning, DataflowConfiguration
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning, DataflowConfiguration
from airflow.utils.decorators import apply_defaults
from airflow_options.utils import TryExcept
from airflow.models import Variable
from airflow_custom_operators.gcs_to_bq import GCSToSnowflakeOperator

class GcsToSFPipeline(BaseOperator, BeamDataflowMixin):
    @apply_defaults
    def __init__(
            self,
            xcom_task_id: str,
            view: str,
            task_id: str,
            runner: str,
            jar: str,
            project: str,
            service_account: str,
            region: str,
            gcp_bucket: str,
            num_workers: int,
            max_num_workers: int,
            worker_machine_type: str,
            subnetwork: str,
            use_public_ips: bool,
            snowflake_server_name: str,
            snowflake_database: str,
            snowflake_schema: str,
            snowflake_table: str,
            snowflake_warehouse: str,
            snowflake_user_role: str,
            snowflake_username: str,
            snowflake_password: str,
            storage_integration: str,
            staging_bucket: str,
            tenant: str,
            pipeline: str,
            autoscaling_algorithm: str = "THROUGHPUT_BASED",
            default_file_encoding: str = "None",
            job_class: Optional[str] = "com.impact.GcsToSnowflakePipeline",
            default_pipeline_options: Optional[dict] = None,
            pipeline_options: Optional[dict] = None,
            delegate_to: Optional[str] = None,
            *args, **kwargs):
        
        self.runner = runner
        self.xcom_task_id = xcom_task_id
        self.view = view
        self.task_id = task_id
        self.project = project
        self.service_account = service_account
        self.region = region
        self.gcp_bucket = gcp_bucket
        self.num_workers = num_workers
        self.max_num_workers = max_num_workers
        self.worker_machine_type = worker_machine_type
        self.subnetwork = subnetwork
        self.use_public_ips = use_public_ips
        self.snowflake_server_name = snowflake_server_name
        self.snowflake_database = snowflake_database
        self.snowflake_schema = snowflake_schema
        self.snowflake_table = snowflake_table
        self.snowflake_warehouse = snowflake_warehouse
        self.snowflake_user_role = snowflake_user_role
        self.snowflake_username = snowflake_username
        self.snowflake_password = snowflake_password
        self.storage_integration = storage_integration
        self.staging_bucket = staging_bucket
        self.autoscaling_algorithm = autoscaling_algorithm
        self.default_file_encoding = default_file_encoding
        self.jar = f"gs://dataflow-uber-jars/{jar}"
        self.job_class = job_class
        self.tenant = tenant
        self.pipeline = pipeline
        self.default_pipeline_options = default_pipeline_options or {}
        self.pipeline_options = pipeline_options or {}
        self.delegate_to = delegate_to
        self.dataflow_job_id = None
        self.dataflow_hook: Optional[DataflowHook] = None
        self.beam_hook: Optional[BeamHook] = None
        self.normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', self.view)
        self._dataflow_job_name = f"gcs-to-snowflake-{self.normalized_view_name.replace('_', '-').lower()}-{tenant.lower().replace('_', '-')}-{pipeline}"
        super().__init__(task_id=task_id, *args, **kwargs)

    @staticmethod
    def _get_folder_date_pattern_from_config(config: dict):
        import re
        file_pattern:str = config.get("file_pattern", "yyyy-MM-dd/{view}_\d+.*.(csv|dat|txt|csv.gz|dat.gz|txt.gz|gz|parquet)")
        date_match = re.search("(yyyy-MM-dd|yyyyMMdd|yyMMdd)", file_pattern)
        if date_match:
            return date_match.group(0)
        else:
            return "yyyy-MM-dd"
    
    @staticmethod
    def _map_date_pattern_to_py_format(date_pattern: str):
        py_date_patterns_map = {
            "yyyy-MM-dd" : "%Y-%m-%d",
            "yyyyMMdd" : "%Y%m%d",
            "yyMMdd" : "%y%m%d"
        }
        return py_date_patterns_map.get(date_pattern, "%Y-%m-%d")

    @staticmethod
    def _get_last_processed_date(view:str):
        import re
        from airflow.models import Variable
        if view not in eval(Variable.get("view_trigger_mandatory_mapping", default_var="{}")):
            if re.search("\.\d{6}", Variable.get(f"{view}_param_extraction_sync_dt")):
                return Variable.get(f"{view}_param_extraction_sync_dt")[:-3]
            else:
                return Variable.get(f"{view}_param_extraction_sync_dt")
        return ""

    @TryExcept
    def execute(self, context):
        from datetime import datetime, timedelta
        # Fetch XCom data
        self.dataflow_config = {
            "poll_sleep": 10,
            "job_name": self._dataflow_job_name,
            "location": self.region
        }
        xcom_data_source_config = self.xcom_pull(context=context, dag_id=context.get('dag').dag_id, task_ids=self.xcom_task_id, key='return_value')
        self.audit_column = Variable.get(f"{self.view}_param_filter", default_var="SYNCSTARTDATETIME")
        if str(Variable.get(f"{self.view}_param_replace", default_var="False"))=="True":
            self.replace_table = True
        else:
            self.replace_table = False
        self.directories = eval(Variable.get(f"{self.view}_dirs_with_trigger_file", default_var="None"))
        self.last_processed_date = self._get_last_processed_date(self.view)
        self.bucket = xcom_data_source_config.get('bucket')
        self.gcs_blob_path_prefix = xcom_data_source_config.get('blob_prefix')
        self.file_prefix = f"{Variable.get(f'{self.view}_file_prefix', default_var=f'{self.view}')}"
        self.file_suffix =  Variable.get(f"{self.view}_file_suffix", default_var="(csv|dat|txt|csv.gz|dat.gz|txt.gz|gz|parquet)")
        self.folder_date_pattern = f"{Variable.get(f'{self.view}_folder_date_pattern', default_var=self._get_folder_date_pattern_from_config(xcom_data_source_config))}"
        self.folder_date_pattern_py = f"{Variable.get(f'{self.view}_folder_date_pattern_py', default_var=self._map_date_pattern_to_py_format(self.folder_date_pattern))}"
        self.field_delimiter = Variable.get(f"{self.view}_param_field_delimiter", default_var="|")
        self.pull_type = Variable.get(f"{self.view}_param_pull_type", default_var="incremental")

        self.sf_clustering_columns = Variable.get(f"{self.view}_param_clustering_columns", default_var="None")
        if self.sf_clustering_columns == "None":
            self.sf_clustering_columns = "null"
        else:
            self.sf_clustering_columns = ",".join(eval(self.sf_clustering_columns))
        if self.default_file_encoding == "None":
            self.default_file_encoding = "null"

        self.temp_location = f"gs://{self.gcp_bucket}/dataflow/temp/"
        self.staging_location = f"gs://{self.gcp_bucket}/dataflow/staging/"

        self.replace_special_chars = eval(str(Variable.get(f"{self.view}_param_replace_special_characters", default_var="False")))

        miscellaneous_attributes_json = eval(Variable.get(f"{self.view}_param_miscellaneous_attributes", default_var="None"))
        self.use_standard_csv_parser = "true"
        if miscellaneous_attributes_json:
             self.use_standard_csv_parser = miscellaneous_attributes_json.get("use_standard_csv_parser", "true")

        if self.replace_special_chars:
            self.beam_hook = BeamHook(runner=self.runner)
            self.pipeline_options = {
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
                "snowflakeTable" : self.snowflake_table,
                "storageIntegration": self.storage_integration ,
                "stagingBucket" : self.staging_bucket ,
                "snowflakeServerName": self.snowflake_server_name ,
                "snowflakeUserName": self.snowflake_username ,
                "snowflakePassword": self.snowflake_password ,
                "snowflakeDatabase": self.snowflake_database ,
                "snowflakeSchema": self.snowflake_schema ,
                "snowflakeWarehouse": self.snowflake_warehouse ,
                "snowflakeUserRole": self.snowflake_user_role ,
                "replaceTable": str(self.replace_table).lower(),
                "directories": ";".join(self.directories) if self.directories else "null",
                "lastProcessedDate": self.last_processed_date,
                "gcsBucketName": self.bucket,
                "gcsBlobPathPrefix": self.gcs_blob_path_prefix,
                "blobNamePrefix": self.file_prefix,
                "blobNameSuffix": self.file_suffix,
                "folderDatePattern": self.folder_date_pattern,
                "auditColumn": self.audit_column,
                "fieldDelimiter": self.field_delimiter or "|",
                "pullType": self.pull_type,
                "snowflakeClusteringColumns": self.sf_clustering_columns,
                "replaceSpecialChars": str(self.replace_special_chars).lower(),
                "fileEncoding": self.default_file_encoding,
                "useStandardCsvParser" : self.use_standard_csv_parser
            }
            process_line_callback: Optional[Callable] = None
            pipeline_options = self.pipeline_options.copy()
            is_dataflow = self.runner.lower() == BeamRunnerType.DataflowRunner.lower()
            dataflow_job_name: str = None

            if isinstance(self.dataflow_config, dict):
                self.dataflow_config = DataflowConfiguration(**self.dataflow_config)

            if is_dataflow:
                dataflow_job_name, pipeline_options, process_line_callback = self._set_dataflow(
                    pipeline_options=pipeline_options, job_name_variable_key=None
                )

            pipeline_options.update(self.pipeline_options)

            with ExitStack() as exit_stack:
                if self.jar.lower().startswith("gs://"):
                    gcs_hook = GCSHook(use_default_project_id=True, project_id=self.project)
                    tmp_gcs_file = exit_stack.enter_context(gcs_hook.provide_file(object_url=self.jar))
                    self.jar = tmp_gcs_file.name

                if is_dataflow:
                    is_running = False
                    if self.dataflow_config.check_if_running != CheckJobRunning.IgnoreJob:
                        is_running = (
                            # The reason for disable=no-value-for-parameter is that project_id parameter is
                            # required but here is not passed, moreover it cannot be passed here.
                            # This method is wrapped by @_fallback_to_project_id_from_variables decorator which
                            # fallback project_id value from variables and raise error if project_id is
                            # defined both in variables and as parameter (here is already defined in variables)
                            self.dataflow_hook.is_job_dataflow_running(
                                name=self.dataflow_config.job_name,
                                variables=pipeline_options,
                            )
                        )
                        while is_running and self.dataflow_config.check_if_running == CheckJobRunning.WaitForRun:
                            # The reason for disable=no-value-for-parameter is that project_id parameter is
                            # required but here is not passed, moreover it cannot be passed here.
                            # This method is wrapped by @_fallback_to_project_id_from_variables decorator which
                            # fallback project_id value from variables and raise error if project_id is
                            # defined both in variables and as parameter (here is already defined in variables)

                            is_running = self.dataflow_hook.is_job_dataflow_running(
                                name=self.dataflow_config.job_name,
                                variables=pipeline_options,
                            )
                    if not is_running:
                        pipeline_options["jobName"] = dataflow_job_name
                        with self.dataflow_hook.provide_authorized_gcloud():
                            self.beam_hook.start_java_pipeline(
                                variables=pipeline_options,
                                jar=self.jar,
                                job_class=self.job_class,
                                process_line_callback=process_line_callback,
                            )
                        self.dataflow_hook.wait_for_done(
                            job_name=dataflow_job_name,
                            location=self.dataflow_config.location,
                            job_id=self.dataflow_job_id,
                            multiple_jobs=self.dataflow_config.multiple_jobs,
                            project_id=self.dataflow_config.project_id,
                        )

            return {"dataflow_job_id": self.dataflow_job_id}
        else:
            self.pipeline_obj = GCSToSnowflakeOperator(
                task_id=self.task_id,
                project_id=self.project,
                snowflakeTable=self.snowflake_table,
                storageIntegration=self.storage_integration,
                stagingBucket= self.staging_bucket,
                snowflakeServerName= self.snowflake_server_name,
                snowflakeUserName=self.snowflake_username,
                snowflakePassword=self.snowflake_password,
                snowflakeDatabase=self.snowflake_database,
                snowflakeSchema=self.snowflake_schema,
                snowflakeWarehouse=self.snowflake_warehouse,
                snowflakeUserRole=self.snowflake_user_role,
                bucket_name=self.bucket,
                blob_prefix=self.gcs_blob_path_prefix,
                prefix=self.view,
                suffix=self.file_suffix,
                start_offset=(datetime.fromisoformat(Variable.get(f"{self.view}_param_extraction_sync_dt")) + timedelta(days=1)).strftime("%Y-%m-%d"),
                replace=self.replace_table,
                clustering_columns=self.sf_clustering_columns.split(",") if self.sf_clustering_columns!="null" else None,
                field_delimiter=self.field_delimiter,
                pull_type=self.pull_type,
                directories=self.directories,
                folder_date_pattern=self.folder_date_pattern_py,
                bigquery_region=self.bigquery_region
            )

            return self.pipeline_obj.execute(context)
    
    def on_kill(self) -> None:
        if self.dataflow_hook and self.dataflow_job_id:
            self.log.info('Dataflow job with id: `%s` was requested to be cancelled.', self.dataflow_job_id)
            self.dataflow_hook.cancel_job(
                job_id=self.dataflow_job_id,
                project_id=self.dataflow_config.project_id,
            )
