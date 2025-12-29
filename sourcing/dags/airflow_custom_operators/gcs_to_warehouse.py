from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
import re


class GcsToWarehouseOperator(BaseOperator):
    template_fields = [
        "project",
        "service_account",
        "region",
        "tenant",
        "pipeline",
        "worker_machine_type",
        "subnetwork",
        "autoscaling_algorithm",
        "gcp_bucket",
        "default_file_encoding",
        "xcom_task_id"
    ]

    @apply_defaults
    def __init__(
        self,
        project,
        xcom_task_id,
        view,
        runner, 
        tenant,
        pipeline,
        service_account,
        region,
        worker_machine_type,
        num_workers,
        max_num_workers,
        subnetwork,
        use_public_ips,
        autoscaling_algorithm="THROUGHPUT_BASED",
        gcp_bucket=None,
        default_file_encoding="None",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.project = project
        self.view = view
        self.xcom_task_id = xcom_task_id
        self.service_account = service_account
        self.runner = runner
        self.tenant = tenant
        self.pipeline = pipeline
        self.region = region
        self.worker_machine_type = worker_machine_type
        self.subnetwork = subnetwork
        self.use_public_ips = use_public_ips
        self.autoscaling_algorithm = autoscaling_algorithm
        self.gcp_bucket = gcp_bucket
        self.default_file_encoding = default_file_encoding
        self.num_workers = num_workers
        self.max_num_workers = max_num_workers

    def _render_template_fields(self, context, fields):
        """Render template fields."""
        for field in fields:
            setattr(
                self,
                field,
                self.render_template(getattr(self, field), context)
            )

    def execute(self, context):
        self._render_template_fields(context, self.template_fields)
        warehouse = Variable.get("warehouse", default_var="BIGQUERY").upper()
        normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', self.view)

        if warehouse == "BIGQUERY":
            from airflow_custom_operators.gcs_to_bq_branch import GcsToBQPipeline
            from airflow_options.constants import GCS_TO_BQ_JAR
            
            operator = GcsToBQPipeline(
                task_id=f"gcs_pull_{normalized_view_name}_to_warehouse",
                xcom_task_id=self.xcom_task_id,
                view=self.view,
                runner=self.runner,
                jar=GCS_TO_BQ_JAR,
                tenant=self.tenant,
                pipeline=self.pipeline,
                project=self.project,
                billing_project=Variable.get('billing_project_id'),
                service_account=self.service_account,
                region=self.region,
                bigquery_region=Variable.get('bigquery_region', 'US'),
                gcp_bucket=self.gcp_bucket,
                num_workers=self.num_workers,
                max_num_workers=self.max_num_workers,
                worker_machine_type=self.worker_machine_type,
                subnetwork=self.subnetwork,
                use_public_ips=self.use_public_ips,
                bigquery_project=Variable.get('gcp_project'),
                bigquery_dataset=Variable.get('gcp_dataset'),
                bigquery_table=normalized_view_name,
                default_file_encoding=self.default_file_encoding,
                autoscaling_algorithm=self.autoscaling_algorithm
            )
        elif warehouse == "SNOWFLAKE":
            from airflow_custom_operators.gcs_to_sf_branch import GcsToSFPipeline
            from airflow_options.constants import GCS_TO_SF_JAR
            
            operator = GcsToSFPipeline(
                task_id=f"gcs_pull_{normalized_view_name}_to_snowflake",
                xcom_task_id=self.xcom_task_id,
                view=self.view,
                runner=self.runner,
                jar=GCS_TO_SF_JAR,
                tenant=self.tenant,
                pipeline=self.pipeline,
                project=self.project,
                service_account=self.service_account,
                region=self.region,
                gcp_bucket=self.gcp_bucket or Variable.get('gcp_bucket'),
                num_workers=int(Variable.get('num_workers', 2)),
                max_num_workers=int(Variable.get('max_num_workers', 4)),
                worker_machine_type=self.worker_machine_type,
                subnetwork=self.subnetwork,
                use_public_ips=self.use_public_ips,
                snowflake_server_name=Variable.get('snowflake_server_name'),
                snowflake_database=Variable.get('snowflake_database'),
                snowflake_schema=Variable.get('snowflake_schema'),
                snowflake_table=normalized_view_name,
                snowflake_warehouse=Variable.get('snowflake_warehouse'),
                snowflake_user_role=Variable.get('snowflake_user_role'),
                snowflake_username=Variable.get('snowflake_username'),
                snowflake_password=Variable.get('snowflake_password'),
                storage_integration=Variable.get('storage_integration'),
                staging_bucket=Variable.get('staging_bucket'),
                default_file_encoding=self.default_file_encoding,
                autoscaling_algorithm=self.autoscaling_algorithm
            )
        else:
            raise AirflowFailException(f"Unsupported warehouse type: {warehouse}")

        return operator.execute(context)