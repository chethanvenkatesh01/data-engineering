from typing import Optional, Callable
from airflow.models import BaseOperator
from contextlib import ExitStack
from airflow.providers.apache.beam.operators.beam import BeamDataflowMixin
from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning, DataflowConfiguration
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning, DataflowConfiguration
from airflow_custom_operators.snowflake import SnowflakeToBigQueryOperator
from airflow.utils.decorators import apply_defaults
from airflow_options.utils import TryExcept
from airflow.models import Variable

class SnowflakeToBQPipeline(BaseOperator, BeamDataflowMixin):

    template_fields = [
        "project",
        "billing_project",
        "service_account",
        "region",
        "bigquery_region",
        "temp_location",
        "staging_location",
        "worker_machine_type",
        "subnetwork",
        "bigquery_project",
        "bigquery_dataset",
        "autoscaling_algorithm",
        "xcom_task_id",
    ]

    @apply_defaults
    def __init__(
            self,
            xcom_task_id: str,
            view: str,
            task_id: str,
            runner: str,
            jar: str,
            project: str,
            billing_project: str,
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
            bigquery_project: str,
            bigquery_dataset: str,
            bigquery_table: str,
            tenant: str,
            pipeline: str,
            autoscaling_algorithm: str = "THROUGHPUT_BASED",
            job_class: Optional[str] = "com.impact.SnowflakeToBigQueryPipeline",
            default_pipeline_options: Optional[dict] = None,
            pipeline_options: Optional[dict] = None,
            gcp_conn_id: str = "google_cloud_default",
            delegate_to: Optional[str] = None,
            *args, **kwargs):
        self.runner = runner
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
        self.bigquery_project = bigquery_project
        self.bigquery_dataset = bigquery_dataset
        self.bigquery_table = bigquery_table
        self.snowflake_table = view
        self.autoscaling_algorithm = autoscaling_algorithm
        self.jar = f"gs://dataflow-uber-jars/{jar}"
        self.tenant = tenant
        self.pipeline = pipeline
        self.job_class = job_class
        self.default_pipeline_options = default_pipeline_options or {}
        self.pipeline_options = pipeline_options or {}
        self.delegate_to = delegate_to
        self.dataflow_job_id = None
        self.dataflow_hook: Optional[DataflowHook] = None
        self.beam_hook: Optional[BeamHook] = None
        self._dataflow_job_name = f"snowflake-pull-{self.view.replace('_', '-').lower()}-bq-{self.tenant.lower().replace('_', '-')}-{self.pipeline}"
        self.gcp_conn_id = gcp_conn_id
        super().__init__(task_id=task_id, *args, **kwargs)

    def _render_template_fields(self, context, fields):
        """Render template fields."""
        for field in fields:
            setattr(
                self,
                field,
                self.render_template(getattr(self, field), context)
            )

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
        import re

        # Render template variables
        self._render_template_fields(context, self.template_fields)
        
        # 
        self.dataflow_config = {
            "poll_sleep": 10,
            "job_name": self._dataflow_job_name,
            "location": self.region
        }

        # Fetch XCom data
        xcom_data_source_config = self.xcom_pull(context=context, dag_id=context.get('dag').dag_id, task_ids=self.xcom_task_id, key='return_value')
        self.warehouse = xcom_data_source_config.get('warehouse')
        self.role = xcom_data_source_config.get('role', 'ACCOUNTADMIN')
        self.database = xcom_data_source_config.get('database')
        self.schema = xcom_data_source_config.get('schema')
        self.staging_bucket = f"gs://{Variable.get('gcp_bucket')}/"

        self.audit_column = Variable.get(f"{self.view}_param_filter", default_var="SYNCSTARTDATETIME")
        if str(Variable.get(f"{self.view}_param_replace", default_var="False"))=="True":
            self.replace_table = True
        else:
            self.replace_table = False
        self.last_processed_date = self._get_last_processed_date(self.view)
        self.pull_type = Variable.get(f"{self.view}_param_pull_type", default_var="incremental")
        self.bq_partition_column = Variable.get(f"{self.view}_param_partition_column", default_var="None")
        if self.bq_partition_column == "None":
            self.bq_partition_column = "null"
        self.bq_clustering_columns = Variable.get(f"{self.view}_param_clustering_columns", default_var="None")
        if self.bq_clustering_columns == "None":
            self.bq_clustering_columns = "null"
        else:
            self.bq_clustering_columns = ",".join(eval(self.bq_clustering_columns))
        # Adding default values as "null" so it'll be passed as java nulls
        self.incremental_column = Variable.get(f"{self.view}_param_filter", default_var="null")
        self.incremental_column_value  = Variable.get(f"{self.view}_param_extraction_sync_dt", default_var="null")
        if re.search("\.\d{6}", self.incremental_column_value):
            self.incremental_column_value = self.incremental_column_value[:-3]
        self.snowflake_secret = Variable.get(f"{self.view}_param_source_config", default_var="null")

        self.replace_special_chars = eval(str(Variable.get(f"{self.view}_param_replace_special_characters", default_var="False")))
        if self.replace_special_chars:
            self.beam_hook = BeamHook(runner=self.runner)
            self.pipeline_options={
                "project": self.project,
                "bigqueryBillingProject": self.billing_project,
                "serviceAccount": self.service_account,
                "region": self.region,
                "bigqueryRegion": self.bigquery_region,
                "tempLocation": self.temp_location,
                "stagingLocation": self.staging_location,
                "stagingBucket": self.staging_bucket,
                "autoscalingAlgorithm": "THROUGHPUT_BASED",
                "numWorkers": self.num_workers,
                "maxNumWorkers": self.max_num_workers,
                "workerMachineType": self.worker_machine_type,
                "subnetwork": self.subnetwork,
                "usePublicIps": self.use_public_ips,
                "bigqueryProject": self.bigquery_project,
                "bigqueryDataset": self.bigquery_dataset,
                "bigqueryTable": self.view,
                "snowflakeTable": self.snowflake_table,
                "replaceTable": str(self.replace_table).lower(),
                "auditColumn": self.audit_column,
                "pullType": self.pull_type,
                "bqPartitionColumn": self.bq_partition_column,
                "bqClusteringColumns": self.bq_clustering_columns,
                "incrementalColumn": self.incremental_column,
                "incrementalColumnValue": self.incremental_column_value,
                "replaceSpecialChars": str(self.replace_special_chars).lower(),
                "snowflakeSecretName": self.snowflake_secret
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
                    gcs_hook = GCSHook(self.gcp_conn_id, self.delegate_to)
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
            self.pipeline_obj = SnowflakeToBigQueryOperator(
                task_id=self.task_id,
                bq_dataset=self.bigquery_dataset,
                bq_project=self.project,
                bq_table=self.bigquery_table,
                staging_gcs_location=self.staging_bucket,
                config_secret_name=self.snowflake_secret if self.snowflake_secret!="null" else None,
                table=self.view,
                warehouse=self.warehouse,
                role=self.role,
                database=self.database,
                schema=self.schema,
                staging_file_prefix=self.view,
                staging_file_format="parquet",
                overwrite_files=True,
                replace_destination_table=self.replace_table,
                pull_type=self.pull_type,
                incremental_column=self.incremental_column if self.incremental_column!="null" else None,
                incremental_column_value=self.incremental_column_value if self.incremental_column_value!="null" else None,
                bq_partition_column=self.bq_partition_column if self.bq_partition_column!="null" else None,
                bq_clustering_columns=self.bq_clustering_columns.split(",") if self.bq_clustering_columns!="null" else None,
                audit_column_name=self.audit_column,
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
