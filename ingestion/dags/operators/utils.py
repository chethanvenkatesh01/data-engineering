def set_warehouse_kwargs():
    from airflow.models import Variable

    if Variable.get("warehouse") == "SNOWFLAKE":
        warehouse_kwargs = {
            "conn_type": "snowflake",
            "conn_id": "warehouse_connection",
            "host": Variable.get("sf_host"),
            "database": Variable.get("sf_database"),
            "schema": Variable.get("sf_schema"),
            "user": Variable.get("sf_user"),
            "password": Variable.get("sf_password"),
            "account": Variable.get("sf_account"),
            "warehouse": Variable.get("sf_warehouse"),
            "role": Variable.get("sf_role"),
            "region": Variable.get("sf_region"),
            "storage_integration": Variable.get("sf_integration"),
        }
    elif Variable.get("warehouse") == "BIGQUERY":
        warehouse_kwargs = {
            "conn_type": "google_cloud_platform",
            "conn_id": "warehouse_connection",
            "project": Variable.get("gcp_project"),
            "billing_project": Variable.get("billing_project_id"),
            "dataset": Variable.get("dataset"),
            "reading_project": Variable.get("reading_project"),
            "reading_dataset": Variable.get("reading_dataset"),
            "bigquery_region": Variable.get("bigquery_region"),
            "region": Variable.get("region"),
        }
    else:
        raise ValueError("Unsupported warehouse type")
    Variable.set("warehouse_kwargs", warehouse_kwargs)
    return warehouse_kwargs
