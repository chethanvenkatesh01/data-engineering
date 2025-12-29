from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import re



class DbToWarehouseOperator(BaseOperator):
    template_fields = [
        "project",
        "service_account",
        "region",
        "temp_location",
        "staging_location",
        "worker_machine_type",
        "subnetwork",
        "xcom_task_id"
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
        db_table,
        query_partitioning_threshold,
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
        self.db_table = db_table
        self.query_partitioning_threshold = query_partitioning_threshold

    def _render_template_fields(self, context, fields):
        """Render template fields."""
        for field in fields:
            setattr(
                self,
                field,
                self.render_template(getattr(self, field), context)
            )

    def execute(self, context):
        # Render template variables
        self._render_template_fields(context, self.template_fields)
        warehouse = Variable.get("warehouse", default_var="BIGQUERY").upper()
        normalized_view_name = re.sub('[^A-Za-z0-9_]', '_', self.view)

        if warehouse == "BIGQUERY":
            from airflow_custom_operators.db_to_bq import DbToBQPipeline
            from airflow_options.constants import DB_TO_BQ_JAR

            self.jar = DB_TO_BQ_JAR
            operator = DbToBQPipeline(
                task_id=f"db_pull_{normalized_view_name}_to_warehouse",
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
                db_table=self.db_table,
                bigquery_dataset=Variable.get('gcp_dataset'),
                bigquery_table=normalized_view_name,
                query_partitioning_threshold=self.query_partitioning_threshold
            )
        
        elif warehouse == "SNOWFLAKE":
            from airflow_custom_operators.db_to_snowflake import DbToSnowflakePipeline
            from airflow_options.constants import DB_TO_SF_JAR

            self.jar = DB_TO_SF_JAR
            operator = DbToSnowflakePipeline(
                task_id=f"db_pull_{normalized_view_name}_to_snowflake",
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
                db_table=self.db_table,
                SnowflakeSecretName=Variable.get('warehouse_secret_name'),
                query_partitioning_threshold=self.query_partitioning_threshold
            )
        else:
            raise AirflowFailException(f"Unsupported warehouse type: {warehouse}")

        return operator.execute(context)
