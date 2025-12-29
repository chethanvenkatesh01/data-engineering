def load_initial_variables():
    """
    Read from db
    """

    from subprocess import run
    from os import getenv, environ
    from dotenv import load_dotenv
    from airflow.models import Variable
    from tasks.utils import connect_to_db_and_execute_query
    from constants.constant import get_overall_config
    from operators.utils import set_warehouse_kwargs

    def set_pg_vars():

        from google.cloud.secretmanager import SecretManagerServiceClient

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
        Variable.set("gcp_project", getenv("GCP_PROJECT"))
        Variable.set("tenant", getenv("tenant"))
        Variable.set("pipeline", getenv("pipeline"))

        # read the postgres creadentials to load the initial variables from secrets
        client = SecretManagerServiceClient()
        name = f"projects/{getenv('GCP_PROJECT')}/secrets/{getenv('tenant')}_{getenv('pipeline')}_data_ingestion/versions/latest"
        response = client.access_secret_version(name=name)
        tenant_config: dict = eval(response.payload.data.decode("UTF-8"))

        Variable.set("PGXUSER", tenant_config["pg_user"])
        Variable.set("PGXPASSWORD", tenant_config["pg_password"])
        Variable.set("PGXHOST", tenant_config["pg_host"])
        Variable.set("PGXDATABASE", tenant_config["pg_database"])
        Variable.set("PGXPORT", tenant_config["pg_port"])
        Variable.set("PGXSCHEMA", tenant_config["pg_schema"])

        # set tenant-alias
        tenant_alias = tenant_config.get("tenant-alias") or getenv("tenant")
        Variable.set("tenant_alias", tenant_alias)

    def set_warehouse_vars():
        from google.cloud.secretmanager import SecretManagerServiceClient

        if getenv("WAREHOUSE", "BIGQUERY") == "SNOWFLAKE":
            Variable.set("warehouse", "SNOWFLAKE")
            client = SecretManagerServiceClient()
            name = f"projects/{getenv('GCP_PROJECT')}/secrets/{getenv('tenant')}_{getenv('pipeline')}_data_ingestion_warehouse/versions/latest"
            response = client.access_secret_version(name=name)
            sf_config: dict = eval(response.payload.data.decode("UTF-8"))

            Variable.set("sf_user", sf_config["sf_user"])
            Variable.set("sf_password", sf_config["sf_password"])
            Variable.set("sf_account", sf_config["sf_account"])
            Variable.set("sf_warehouse", sf_config["sf_warehouse"])
            Variable.set("sf_database", sf_config["sf_database"])
            Variable.set("sf_schema", sf_config["sf_schema"])
            Variable.set("sf_role", sf_config["sf_role"])
            Variable.set("sf_region", sf_config["sf_region"])
            Variable.set("sf_stage", sf_config["sf_stage"])
            Variable.set("sf_bucket", sf_config["sf_staging_bucket"])
            Variable.set("sf_integration", sf_config["sf_storage_integration"])
            Variable.set("sf_host", sf_config["sf_host"])
        elif getenv("WAREHOUSE", "BIGQUERY") == "BIGQUERY":
            Variable.set("warehouse", "BIGQUERY")

        # read the snowflake cread

    def set_default_values():
        default_values_dict = {
            "override_master_table_query": "False",
            "override_lsi_query": "False",
            "override_fmt_query": "False",
            "model_refresh": "False",
            "slack_channel_name": getenv("tenant"),
            "ignore_data_loss_threshold": "True",
            "anomaly_data_file_type": "csv",
            "anomaly_data_file_delimiter": "|",
            "share_anomaly_data": "False",
            "product": "ImpactSmartSuite",
            "run_lsi_flag": "True",
            "dag_timezone": "UTC"
        }

        # these might be present as default empty entries through data-platform UI
        for key, value in default_values_dict.items():
            try:
                Variable.get(key)
            except KeyError:
                Variable.set(key, value)

    def read_from_db():

        sub_modules_ingestion = [
            "'core_ingestion_configuration'",
            "'validation_transformation_ingestion_configuration'",
            "'derived_tables_ingestion_configuration'",
            "'mlops_ingestion_configuration'",
            "'notification_ingestion_configuration'",
        ]
        sub_modules_ingestion = ",".join(sub_modules_ingestion)
        query = f"SELECT attribute_name,attribute_value FROM data_platform.data_ingestion_config WHERE module in ({sub_modules_ingestion}) AND is_latest=True AND is_deleted = false;"

        result_df = connect_to_db_and_execute_query(query)

        result = result_df.to_dict("records")

        for each_row in result:
            Variable.set(each_row["attribute_name"], each_row["attribute_value"])

        set_default_values()
        set_billing_project()

    def read_from_env():
        tenant_alias = Variable.get("tenant_alias")
        Variable.set("tenant", getenv("tenant"))
        Variable.set("pipeline", getenv("pipeline"))
        Variable.set("dataset", getenv("GBQ_DATASET"))
        Variable.set("gcp_project", getenv("GCP_PROJECT"))
        Variable.set("reading_dataset", getenv("READING_DATASET"))
        Variable.set("reading_project", getenv("READING_PROJECT"))
        Variable.set("service_account_email", getenv("service_account_email"))
        Variable.set("runner", getenv("runner", "DataFlowRunner"))
        Variable.set("tempbucket", f"""{tenant_alias}-ingestion-{getenv("pipeline")}""")
        Variable.set("region", getenv("region"))
        Variable.set("bigquery_region", getenv("bigquery_region", default="US"))
        Variable.set("drivername", str(getenv("drivername")))
        Variable.set("network", getenv("network"))
        Variable.set("subnetwork", getenv("subnetwork"))
        Variable.set("machine_type", getenv("machine_type"))

        # inputs
        Variable.set("ingestion_type", str(getenv("ingestion_type")))
        Variable.set("fiscal_start_date", str(getenv("fiscal_start_date")))
        Variable.set("aggregation_period", str(getenv("aggregation_period")))
        Variable.set("rule_master_name", str(getenv("rule_master_name")))
        Variable.set("kpi_master_name", str(getenv("kpi_master_name")))
        Variable.set(
            "sourcing_mapping_table_name", str(getenv("sourcing_mapping_table_name"))
        )
        Variable.set(
            "view_mapping_table",
            str(
                getenv(
                    "view_mapping_table",
                    default=Variable.get("sourcing_mapping_table_name"),
                )
            ),
        )
        Variable.set("mapping_table_to_class", str(getenv("mapping_table_to_class")))
        Variable.set("gcs_bucket", f"""{tenant_alias}-ingestion-{getenv("pipeline")}""")
        Variable.set("derived_tables_queries_loc", "derived_tables")
        Variable.set("preprocessing_queries_loc", "preprocessing/queries.json")

        # Variable that determines whether to run store clustering and LSI
        Variable.set("run_lsi_flag", getenv("run_lsi_flag", default="True"))
        Variable.set(
            "run_store_clustering", getenv("run_store_clustering", default="True")
        )

        # store_split
        Variable.set("store_split_level", str(getenv("store_split_level")))

        Variable.set(
            "level_of_store_clustering", str(getenv("level_of_store_clustering"))
        )

        # day split
        Variable.set("modelling_level", str(getenv("modelling_level")))
        Variable.set("day_split_level", str(getenv("day_split_level")))

        # analytics integration
        Variable.set("main_master_cadence", getenv("main_master_cadence"))
        Variable.set("main_master_backfill", getenv("main_master_backfill"))
        Variable.set("main_master_schema_change", getenv("main_master_schema_change"))
        Variable.set("fmt_cadence", getenv("fmt_cadence"))
        Variable.set("fmt_backfill", getenv("fmt_backfill"))
        Variable.set("fmt_schema_change", getenv("fmt_schema_change"))
        Variable.set(
            "override_master_table_query",
            getenv("override_master_table_query", default="False"),
        )
        Variable.set(
            "override_lsi_query", getenv("override_lsi_query", default="False")
        )
        Variable.set(
            "override_fmt_query", getenv("override_fmt_query", default="False")
        )
        Variable.set("model_refresh", getenv("model_refresh", default="False"))

        # slack integration
        Variable.set(
            "slack_channel_name",
            getenv("slack_channel_name", default=getenv("tenant")),
        )

        # notification
        Variable.set("notification_recipient", getenv("notification_recipient"))
        Variable.set("mail_url", getenv("mail_url"))
        Variable.set(
            "ignore_data_loss_threshold",
            getenv("ignore_data_loss_threshold", default="True"),
        )

        # anomaly data persistance
        Variable.set(
            "anomaly_data_file_type", getenv("anomaly_data_file_type", default="csv")
        )
        Variable.set(
            "anomaly_data_file_delimiter",
            getenv("anomaly_data_file_delimiter", default="|"),
        )
        Variable.set(
            "share_anomaly_data", getenv("share_anomaly_data", default="False")
        )

        Variable.set("product", getenv("product", default="ImpactSmartSuite"))
        Variable.set("reclass_flag", getenv("reclass_flag", default="False"))

        # mp_bucket_code variables
        Variable.set(
            "pseudo_hierarchy_level", getenv("pseudo_hierarchy_level", default="None")
        )
        Variable.set(
            "num_products_per_mp_bucket_code",
            getenv("num_products_per_mp_bucket_code", default="200"),
        )
        Variable.set(
            "psuedo_hierarchy_value_length",
            getenv("psuedo_hierarchy_value_length", default="35"),
        )
        Variable.set(
            "generate_mp_bucket_code",
            getenv("generate_mp_bucket_code", default="False"),
        )
        Variable.set("dag_timezone", getenv("dag_timezone", default="UTC"))

    load_dotenv()
    set_pg_vars()
    set_warehouse_vars()
    set_billing_project()
    Variable.set("read_from_db_flag", getenv("read_from_db_flag", "False"))
    if eval(getenv("read_from_db_flag", "False")):
        read_from_db()
    else:
        read_from_env()

    # Create a ingestion configuration dict
    ingestion_configuration = {
        # tenant_info
        "tenant": Variable.get("tenant"),
        "tenant_alias": Variable.get("tenant_alias", Variable.get("tenant")),
        "pipeline": Variable.get("pipeline"),
        # default values
        "owner": "impact",
        "retries": 1,
        "retry_delay_secs": 10,
        "email_on_failure": False,
        "email_on_retry": False,
        # genric config
        "gcp_project": Variable.get("gcp_project", None),
        "dataset" : Variable.get("dataset", None),
        'billing_project_id':Variable.get("billing_project_id", None),

        # one time per run config
        "ingestion_type": Variable.get("ingestion_type", "None"),
        "run_lsi_flag": eval(Variable.get("run_lsi_flag", "True")),
        "main_master_schema_change": eval(
            Variable.get("main_master_schema_change", "False")
        ),
        "check_if_run_master_table": eval(
            Variable.get("check_if_run_master_table", "True")
        ),
        "check_if_run_fmt": eval(Variable.get("check_if_run_fmt", "False")),
        "fmt_schema_change": eval(Variable.get("fmt_schema_change", "False")),
        "model_refresh": eval(Variable.get("model_refresh", "False")),
        "day_split_level": Variable.get("day_split_level", ""),
        "hierarchy_code_for_modelling_level": Variable.get(
            "hierarchy_code_for_modelling_level", "[]"
        ),
        "reclass_flag": eval(Variable.get("reclass_flag", "False")),
        "hierarchy_count": Variable.get("hierarchy_count", 0),
        # dataflow configs
        "runner": Variable.get("runner"),
        "region": Variable.get("region", default_var="US"),
        "bigquery_region": Variable.get("bigquery_region", default_var="US"),
        "tempbucket": Variable.get("tempbucket"),
        "autoscalingAlgorithm": "THROUGHPUT_BASED",
        "numWorkers": 1,
        "maxNumWorkers": 5,
        "workerMachineType": "n2-highmem-4",
        "direct_num_workers": 10,
        "max_num_workers": 10,
        "network": Variable.get("network"),
        "subnetwork": Variable.get("subnetwork"),
        "no_use_public_ips": True,
        "machine_type": Variable.get("machine_type"),
        "service_account_email": Variable.get("service_account_email"),
        "generate_mp_bucket_code": eval(
            Variable.get("generate_mp_bucket_code", "False")
        ),
        "dag_timezone": Variable.get("dag_timezone", "UTC"),
    }
    overall_config = get_overall_config()
    overall_config["ingestion_config"] = ingestion_configuration
    Variable.set(
        "overall_config", 
        overall_config,
        serialize_json=True
    )
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