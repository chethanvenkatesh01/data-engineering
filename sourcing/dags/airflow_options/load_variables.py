from airflow_options.utils import get_cutoff_or_warning_time
from dataflow_options.utils import Logger

log = Logger(__name__)

def set_project_name_dynamically():
    from os import getenv, environ
    from subprocess import run

    # if the deployment is k8s then set the tenant and pipeline dynamically
    if getenv("DEPLOY_ENV", "None") == "k8s":

        TENANT_MODULE_ENV = getenv("TENANT_MODULE_ENV")
        environ["tenant"] = TENANT_MODULE_ENV.split("-")[0]
        environ["pipeline"] = TENANT_MODULE_ENV.split("-")[2]

    # set the project name dynamically from metadata server of deployment
    project_name = run(
        'curl "http://metadata.google.internal/computeMetadata/v1/project/project-id" -H "Metadata-Flavor: Google"',
        shell=True,
        text=True,
        check=True,
        capture_output=True,
    )
    environ["GCP_PROJECT"] = project_name.stdout


def get_tenant_secret():
    import os
    from dotenv import load_dotenv
    from google.cloud import secretmanager

    load_dotenv()
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{os.getenv('GCP_PROJECT')}/secrets/{os.getenv('tenant')}_{os.getenv('pipeline')}_data_ingestion/versions/latest"
    response = client.access_secret_version(name=name)
    tenant_config: dict = eval(response.payload.data.decode("UTF-8"))
    return tenant_config


def set_pg_vars():
    from airflow.models import Variable

    tenant_config = get_tenant_secret()
    Variable.set("PGXUSER", tenant_config["pg_user"])
    Variable.set("PGXPASSWORD", tenant_config["pg_password"])
    Variable.set("PGXHOST", tenant_config["pg_host"])
    Variable.set("PGXDATABASE", tenant_config["pg_database"])
    Variable.set("PGXPORT", tenant_config["pg_port"])
    Variable.set("PGXSCHEMA", tenant_config["pg_schema"])


def set_warehouse_vars():
    from google.cloud.secretmanager import SecretManagerServiceClient
    from os import getenv, environ
    from airflow.models import Variable
    from dataflow_options.utils import Logger

    log = Logger(__name__)

    if getenv("WAREHOUSE", "BIGQUERY") == "SNOWFLAKE":
        Variable.set("warehouse", "SNOWFLAKE")
        client = SecretManagerServiceClient()
        name = f"projects/{getenv('GCP_PROJECT')}/secrets/{getenv('tenant')}_{getenv('pipeline')}_data_ingestion_warehouse/versions/latest"
        # name = "projects/181744539598/secrets/signet_dev_di_data_ingestion/versions/3"
        log.info("Reading Snowflake from {name}")
        response = client.access_secret_version(name=name)
        sf_config: dict = eval(response.payload.data.decode("UTF-8"))

        log.info(f"Snowflake Secret contains: {sf_config}")

        # Commenting out as no access to secret
        Variable.set("warehouse_secret_name", f"{getenv('tenant')}_{getenv('pipeline')}_data_ingestion_warehouse")    
        Variable.set("sf_user", sf_config["sf_user"])
        Variable.set("sf_password", sf_config["sf_password"])
        Variable.set("sf_account", sf_config["sf_account"])
        Variable.set("sf_warehouse", sf_config["sf_warehouse"])
        Variable.set("sf_database", sf_config["sf_database"])
        Variable.set("sf_schema", sf_config["sf_schema"])
        Variable.set("sf_role", sf_config["sf_role"])
        Variable.set("sf_region", sf_config["sf_region"])
        Variable.set("sf_stage", sf_config["sf_stage"])
        Variable.set("sf_bucket", sf_config["sf_bucket"])
        Variable.set("sf_integration", sf_config["sf_integration"])
        Variable.set("sf_host", sf_config["sf_host"])

        """
        Variable.set("sf_user", getenv("sf_user"))
        Variable.set("sf_password", getenv("sf_password"))
        Variable.set("sf_account", getenv("sf_account"))
        Variable.set("sf_warehouse", getenv("sf_warehouse"))
        Variable.set("sf_database", getenv("sf_database"))
        Variable.set("sf_schema", getenv("sf_schema"))
        Variable.set("sf_role", getenv("sf_role"))
        Variable.set("sf_region", getenv("sf_region"))
        Variable.set("sf_stage", getenv("sf_stage"))
        Variable.set("sf_bucket", getenv("sf_bucket"))
        Variable.set("sf_integration", getenv("sf_integration"))
        Variable.set("sf_host", getenv("sf_host"))
        """

    elif getenv("WAREHOUSE", "BIGQUERY") == "BIGQUERY":
        Variable.set("warehouse", "BIGQUERY")



def set_tenant_alias():
    import os
    from airflow.models import Variable
    tenant_config = {}
    try:
        tenant_config = get_tenant_secret()
    except Exception as e:
        log.error(f"Error while fetching tenant-alias from secret: {e}")
    tenant_alias = tenant_config.get('tenant-alias') or os.getenv('tenant')
    # models.Variable.set('tenant_alias', tenant_alias)
    Variable.set('tenant_alias', tenant_alias)


def read_from_db():
    from airflow.models import Variable
    from airflow_options import database_utils as database_utils

    db_host = Variable.get("PGXHOST")  # os.getenv("PGXHOST")
    db_port = Variable.get("PGXPORT")  # os.getenv("PGXPORT")
    db_database = Variable.get("PGXDATABASE")  # os.getenv("PGXDATABASE")
    db_user = Variable.get("PGXUSER")  # os.getenv("PGX_USER")
    db_password = Variable.get("PGXPASSWORD")  # os.getenv("PGXPASSWORD")

    sourcing_sub_modules = [
        "'core_sourcing_configuration'",
        "'trigger_sourcing_configuration'",
        "'extraction_sourcing_configuration'",
        "'intermediate_sourcing_configuration'",
        "'notification_sourcing_configuration'",
    ]
    sourcing_sub_modules = ",".join(sourcing_sub_modules)
    query = f"Select attribute_name,attribute_value from data_platform.data_ingestion_config where module in ({sourcing_sub_modules}) and is_latest=True and is_deleted = false;"

    result_df = database_utils.connect_to_db_and_execute_query(
        db_host, db_port, db_database, db_user, db_password, query
    )
    # result_df.loc[result_df["module"] == "sourcing_configuration"]
    timezone = result_df.loc[
        result_df["attribute_name"] == "dag_timezone", "attribute_value"
    ].values[0]
    result = result_df.to_dict("records")
    for each_row in result:
        if each_row["attribute_name"] in {
            "trigger_cutoff_time",
            "trigger_warning_time",
        }:
            Variable.set(
                each_row["attribute_name"],
                get_cutoff_or_warning_time(each_row["attribute_value"], timezone),
            )
            continue
        Variable.set(each_row["attribute_name"], each_row["attribute_value"])

    set_default_values()
    set_billing_project()


def read_from_env():
    from airflow.models import Variable
    import os
    import json
    from airflow_options.utils import get_schedule

    Variable.set("gcp_project", os.getenv("GCP_PROJECT"))
    Variable.set("gcp_dataset", os.getenv("GCP_DATASET"))

    Variable.set("mapping_table", os.getenv("mapping_table"))
    Variable.set(
        "view_mapping_table",
        os.getenv("view_mapping_table", default=Variable.get("mapping_table")),
    )
    Variable.set("extraction_flag", os.getenv("extraction_flag", default=True))
    Variable.set(
        "intermediate_queries_flag",
        os.getenv("intermediate_queries_flag", default=True),
    )

    Variable.set("service_account_email", os.getenv("service_account_email"))
    # Variable.set('sourcing_creds',os.getenv('sourcing_creds'))
    Variable.set("intermediate_project", os.getenv("intermediate_project"))
    Variable.set("intermediate_dataset", os.getenv("intermediate_dataset"))
    # Variable.set('intermediate_creds',os.getenv('intermediate_creds'))
    Variable.set(
        "gcp_bucket",
        f"{Variable.get('tenant_alias')}-sourcing-{os.getenv('pipeline')}",
    )

    Variable.set("dataflow_runner", os.getenv("dataflow_runner"))
    Variable.set("region", os.getenv("region"))
    Variable.set("bigquery_region", os.getenv("bigquery_region", default="US"))
    Variable.set("temp_location", os.getenv("gcp_bucket"))
    Variable.set("subnetwork", os.getenv("subnetwork"))
    Variable.set("no_use_public_ips", os.getenv("no_use_public_ips"))
    Variable.set("save_main_session", os.getenv("save_main_session"))
    Variable.set("machine_type", os.getenv("machine_type"))
    Variable.set("task_retries", os.getenv("task_retries"))

    Variable.set("intermediate_queries_loc", "intermediate_tables")
    Variable.set("client_schema", os.getenv("client_schema"))
    Variable.set("tenant", os.getenv("tenant"))
    Variable.set("pipeline", os.getenv("pipeline"))
    Variable.set("ingestion_type", os.getenv("ingestion_type"))
    Variable.set("dag_generic_schedule_time", os.getenv("dag_generic_schedule_time"))
    dag_configs = {
        "dag_timezone": os.getenv("dag_timezone", default="UTC"),
        "dag_generic_schedule_time": os.getenv("dag_generic_schedule_time"),
        "next_cron": get_schedule(),
        "max_active_tasks": os.getenv("max_active_tasks", 16),
        "trigger_type": os.getenv("trigger_type"),
        "trigger_timeout": os.getenv("trigger_timeout", default=0),
        "trigger_mapping_table": os.getenv(
            "trigger_mapping_table", default="generic_trigger_mapping"
        ),
        "dag_concurrency": os.getenv("dag_concurrency", default=16),
    }
    Variable.set("dag_configs", json.dumps(dag_configs))
    Variable.set(
        "slack_channel_name",
        os.getenv("slack_channel_name", default=os.getenv("tenant")),
    )

    Variable.set(
        "data_ingestion_dag_id", os.getenv("data_ingestion_dag_id", default="")
    )
    Variable.set(
        "data_ingestion_pipeline_url",
        os.getenv("data_ingestion_pipeline_url", default=""),
    )
    Variable.set("mail_recipients", os.getenv("mail_recipients", default=""))

    Variable.set(
        "notification_recipient", os.getenv("notification_recipient", default="[]")
    )
    Variable.set("mail_url", os.getenv("mail_url", default="[]"))
    Variable.set(
        "allow_special_chars", os.getenv("allow_special_chars", default="True")
    )

    # Trigger variables
    # Variable.set('trigger_config_name', os.getenv('trigger_config_name'))
    Variable.set(
        "trigger_cutoff_time",
        get_cutoff_or_warning_time(
            os.getenv("trigger_cutoff_time"), os.getenv("dag_timezone", "UTC")
        ),
    )
    Variable.set("trigger_type", os.getenv("trigger_type"))
    Variable.set(
        "trigger_success_notification_types",
        os.getenv("trigger_success_notification_types", default="['slack']"),
    )
    Variable.set(
        "trigger_failure_notification_types",
        os.getenv("trigger_failure_notification_types", default="['slack']"),
    )
    Variable.set(
        "send_trigger_success_notification",
        os.getenv("send_trigger_success_notification", default="False"),
    )
    Variable.set(
        "send_trigger_failure_notification",
        os.getenv("send_trigger_failure_notification", default="False"),
    )
    if os.getenv("trigger_warning_time", None):
        Variable.set("send_trigger_warning_mail", "True")
    else:
        Variable.set("send_trigger_warning_mail", "False")
    Variable.set(
        "trigger_warning_time",
        get_cutoff_or_warning_time(
            os.getenv("trigger_warning_time", default="00:00"),
            os.getenv("dag_timezone", "UTC"),
        ),
    )
    Variable.set("product", os.getenv("product", default="ImpactSmartSuite"))
    Variable.set(
        "send_sourcing_notification_mail",
        os.getenv("send_sourcing_notification_mail", default="False"),
    )
    Variable.set(
        "send_sourcing_failure_notification_mail",
        os.getenv("send_sourcing_failure_notification_mail", default="False"),
    )

    Variable.set(
        "allow_special_chars", os.getenv("allow_special_chars", default="True")
    )
    Variable.set(
        "failure_mailing_list", os.getenv("failure_mailing_list", default="[]")
    )
    Variable.set(
        "remote_urls_for_sourcing_dags", os.getenv("remote_urls_for_sourcing_dags")
    ),
    Variable.set("dag_concurrency", os.getenv("dag_concurrency"))
    Variable.set(
        "default_file_encoding", os.getenv("default_file_encoding", default="None")
    )
    Variable.set(
        "use_standard_csv_parser", os.getenv("use_standard_csv_parser", default=True)
    )
    set_billing_project()


def set_default_values():
    from json import dumps
    from airflow.models import Variable
    from airflow_options.utils import get_schedule

    default_values_dict = {
        "view_mapping_table": Variable.get("mapping_table"),
        "extraction_flag": True,
        "intermediate_queries_flag": True,
        "dag_timezone": "UTC",
        "slack_channel_name": Variable.get("tenant"),
        "data_ingestion_dag_id": "",
        "data_ingestion_pipeline_url": "",
        "mail_recipients": "",
        "notification_recipient": "[]",
        "mail_url": "[]",
        "allow_special_chars": "True",
        "trigger_cutoff_time": "00:00",
        "trigger_success_notification_types": "['slack']",
        "trigger_failure_notification_types": "['slack']",
        "send_trigger_success_notification": "False",
        "send_trigger_failure_notification": "False",
        "trigger_warning_time": "00:00",
        "trigger_mapping_table": "generic_trigger_mapping",
        "product": "ImpactSmartSuite",
        "send_sourcing_notification_mail": "False",
        "send_sourcing_failure_notification_mail": "False",
        "failure_mailing_list": "[]",
        "remote_urls_for_sourcing_dags": "[]",
        "client_schema": Variable.get("client_schema", None),
        "default_file_encoding": "None",
        "use_standard_csv_parser": True,
    }

    # these might be present as default empty entries through data-platform UI
    non_mandatory_fields = [
        "view_mapping_table",
        "extraction_flag",
        "intermediate_queries_flag",
        "remote_urls_for_sourcing_dags",
    ]
    for key, value in default_values_dict.items():
        try:
            content = Variable.get(key)
            empty_string_flag = all(c.isspace() for c in content)
            # check if values stored in db are None or empty
            if key in non_mandatory_fields and (empty_string_flag or content is None):
                Variable.set(key, value)
        except KeyError:
            Variable.set(key, value)

    dag_configs = {
        "dag_timezone": Variable.get("dag_timezone", "UTC"),
        "dag_generic_schedule_time": Variable.get("dag_generic_schedule_time", "None"),
        "next_cron": get_schedule(),
        "max_active_tasks": Variable.get("max_active_tasks", 16),
        "trigger_type": Variable.get("trigger_type", "None"),
        "trigger_timeout": Variable.get("trigger_timeout", 0),
        "trigger_mapping_table": Variable.get(
            "trigger_mapping_table", "generic_trigger_mapping"
        ),
        "dag_concurrency": Variable.get("dag_concurrency", 16),
    }
    Variable.set("dag_configs", dumps(dag_configs))


def load_env_constants_sourcing():
    from airflow.models import Variable
    import os
    import pytz
    import json
    from datetime import datetime
    from dotenv import load_dotenv
    from airflow_options.utils import get_overall_config
    from operators.utils import set_warehouse_kwargs

    load_dotenv()
    set_tenant_alias()
    Variable.set("read_from_db_flag", os.getenv("read_from_db_flag", "False"))
    if eval(Variable.get("read_from_db_flag", default_var="False")):
        set_project_name_dynamically()
        set_pg_vars()
        read_from_db()
    else:
        read_from_env()

    set_warehouse_vars()

    # set sourcing configs
    sourcing_configuration = {
        "tenant": Variable.get("tenant"),
        "pipeline": Variable.get("pipeline"),
        # dag configs
        "task_retries": Variable.get("task_retries"),
        "dag_configs": Variable.get(
            "dag_configs",
            default_var={
                "dag_generic_schedule_time": None,
                "next_cron": None,
                "dag_timezone": "UTC",
                "max_active_tasks": 32,
                "dag_concurrency": 32,
            },
            deserialize_json=True,
        ),
        # gcp configs
        "gcp_project": Variable.get("gcp_project"),
        "gcp_dataset": Variable.get("gcp_dataset"),
        "gcp_bucket": Variable.get("gcp_bucket"),
        "service_account_email": Variable.get("service_account_email"),
        "billing_project_id": Variable.get("billing_project_id"),
        # extraction configs
        "ingestion_type": Variable.get("ingestion_type"),
        "mapping_table": Variable.get("mapping_table"),
        "extraction_flag": Variable.get("extraction_flag"),
        "client_schema": Variable.get("client_schema", default_var=None),
        "allow_special_chars": Variable.get("allow_special_chars"),
        # intermediate configs
        "intermediate_queries_flag": Variable.get("intermediate_queries_flag"),
        "intermediate_project": Variable.get("intermediate_project"),
        "intermediate_dataset": Variable.get("intermediate_dataset"),
        "intermediate_queries_loc": Variable.get("intermediate_queries_loc"),
        # dataflow configs
        "dataflow_runner": Variable.get("dataflow_runner"),
        "region": Variable.get("region"),
        "temp_location": Variable.get("gcp_bucket"),
        "subnetwork": Variable.get("subnetwork"),
        "no_use_public_ips": Variable.get("no_use_public_ips"),
        "save_main_session": Variable.get("save_main_session"),
        "machine_type": Variable.get("machine_type"),
        # slack configs
        "slack_channel_name": Variable.get("slack_channel_name"),
        # ingestion dag configs
        "data_ingestion_dag_id": Variable.get("data_ingestion_dag_id"),
        "data_ingestion_pipeline_url": Variable.get("data_ingestion_pipeline_url"),
        # trigger configs
        "trigger_type": Variable.get("trigger_type"),
        "trigger_cutoff_time": Variable.get("trigger_cutoff_time"),
        "trigger_success_notification_types": Variable.get(
            "trigger_success_notification_types"
        ),
        "trigger_failure_notification_types": Variable.get(
            "trigger_failure_notification_types"
        ),
        "send_trigger_success_notification": Variable.get(
            "send_trigger_success_notification"
        ),
        "send_trigger_failure_notification": Variable.get(
            "send_trigger_failure_notification"
        ),
        "trigger_warning_time": Variable.get("trigger_warning_time"),
        # notification mails
        "mail_url": Variable.get("mail_url"),
        "mail_recipients": Variable.get("mail_recipients"),
        "notification_recipient": Variable.get("notification_recipient"),
        "send_sourcing_notification_mail": Variable.get(
            "send_sourcing_notification_mail"
        ),
        "send_sourcing_failure_notification_mail": Variable.get(
            "send_sourcing_failure_notification_mail"
        ),
        "failure_mailing_list": Variable.get("failure_mailing_list"),
        "remote_urls_for_sourcing_dags": Variable.get("remote_urls_for_sourcing_dags"),
    }

    timezone = pytz.timezone(
        sourcing_configuration.get("dag_configs", {}).get("dag_timezone", "UTC")
    )
    curr_datetime = datetime.now(timezone)
    curr_date = curr_datetime.date()
    trigger_cutoff_time = datetime.strptime(
        sourcing_configuration.get("trigger_cutoff_time", "00:00"), "%H:%M"
    ).time()
    trigger_cutoff_datetime = timezone.localize(
        datetime.combine(curr_date, trigger_cutoff_time)
    )
    if curr_datetime >= trigger_cutoff_datetime:
        # Variable.set("trigger_timeout", 0)
        sourcing_configuration["dag_configs"]["trigger_timeout"] = 0
    else:
        sourcing_configuration["dag_configs"]["trigger_timeout"] = (
            trigger_cutoff_datetime - curr_datetime
        ).seconds

    overall_configs = get_overall_config()

    overall_configs["sourcing_config"] = sourcing_configuration
    Variable.set("overall_config", json.dumps(overall_configs))
    set_warehouse_kwargs()

def set_billing_project():
    from airflow.models import Variable
    gcp_project = Variable.get("gcp_project")
    pipeline = Variable.get("pipeline")
    if pipeline in ["dev", "test"]:
        if gcp_project == 'victorias-secret-international':
            Variable.set("billing_project_id", 'np-vs-international-27122024')
        elif gcp_project == 'leslies-pool-supplies-181124':
            Variable.set("billing_project_id", 'np-leslies-pool-supplies311224')
        else:
            Variable.set("billing_project_id", f'np-{gcp_project}')
    elif pipeline in ["uat", "prod"]:
        # uat-vs-inter-27122024 , uat-lp-supplies311224
        if gcp_project == 'victorias-secret-international':
            Variable.set("billing_project_id", 'uat-vs-inter-27122024')
        elif gcp_project == 'leslies-pool-supplies-181124':
            Variable.set("billing_project_id", 'uat-lp-supplies311224')
        else:
            Variable.set("billing_project_id", f'uat-{gcp_project}')
    else:
        Variable.set("billing_project_id", f'{gcp_project}')