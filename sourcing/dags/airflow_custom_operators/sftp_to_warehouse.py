from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
import re


class SftpToWarehouseOperator(BaseOperator):
    template_fields = [
        "project",
        "service_account",
        "region",
        "temp_location",
        "staging_location",
        "worker_machine_type",
        "subnetwork",
        "xcom_task_id",
        "default_file_encoding"
    ]

    @apply_defaults
    def __init__(
        self, 
        xcom_task_id, 
        view, 
        runner, 
        tenant, 
        pipeline, 
        project,  
        service_account, 
        region, 
        temp_location, 
        staging_location, 
        num_workers, 
        max_num_workers, 
        worker_machine_type, 
        subnetwork, 
        use_public_ips, 
        default_file_encoding, 
        header,
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.xcom_task_id = xcom_task_id
        self.view = view
        self.runner = runner
        self.tenant = tenant
        self.pipeline = pipeline
        self.project = project
        self.service_account = service_account
        self.region = region
        self.temp_location = temp_location
        self.staging_location = staging_location
        self.num_workers = num_workers
        self.max_num_workers = max_num_workers
        self.worker_machine_type = worker_machine_type
        self.subnetwork = subnetwork
        self.use_public_ips = use_public_ips
        self.default_file_encoding = default_file_encoding
        self.header = header

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
            from airflow_custom_operators.sftp_to_bq import SftpToBQPipeline
            from airflow_options.constants import SFTP_TO_BQ_JAR
            
            self.jar = SFTP_TO_BQ_JAR
            operator = SftpToBQPipeline(
                task_id=f"sftp_pull_{normalized_view_name}_to_warehouse",
                xcom_task_id=self.xcom_task_id,
                view=self.view,
                runner=self.runner,
                jar=self.jar,
                tenant=self.tenant,
                pipeline=self.pipeline,
                project=self.project,
                billing_project=Variable.get('billing_project_id'),
                service_account=self.service_account,
                region=self.region,
                bigquery_region=Variable.get('bigquery_region', 'US'),
                temp_location=self.temp_location,
                staging_location=self.staging_location,
                num_workers=self.num_workers,
                max_num_workers=self.max_num_workers,
                worker_machine_type=self.worker_machine_type,
                subnetwork=self.subnetwork,
                use_public_ips=self.use_public_ips,
                bigquery_project=Variable.get('gcp_project'),
                bigquery_dataset=Variable.get('gcp_dataset'),
                bigquery_table=normalized_view_name,
                header=self.header,
                default_file_encoding=self.default_file_encoding,
            )
        elif warehouse == "SNOWFLAKE":
            from airflow_custom_operators.sftp_to_snowflake import SftpToSnowflakePipeline
            from airflow_options.constants import SFTP_TO_SF_JAR
            
            self.jar = SFTP_TO_SF_JAR
            operator = SftpToSnowflakePipeline(
                task_id=f"sftp_pull_{normalized_view_name}_to_snowflake",
                xcom_task_id=self.xcom_task_id,
                view=self.view,
                runner=self.runner,
                jar=self.jar,
                tenant=self.tenant,
                pipeline=self.pipeline,
                project=self.project,
                service_account=self.service_account,
                region=self.region,
                temp_location=self.temp_location,
                staging_location=self.staging_location,
                num_workers=self.num_workers,
                max_num_workers=self.max_num_workers,
                worker_machine_type=self.worker_machine_type,
                subnetwork=self.subnetwork,
                use_public_ips=self.use_public_ips,
                snowflake_table=normalized_view_name,
                header=self.header,
                SnowflakeSecretName=Variable.get('warehouse_secret_name'),
                default_file_encoding=self.default_file_encoding,
            )
        else:
            raise AirflowFailException(f"Unsupported warehouse type: {warehouse}")

        return operator.execute(context)
