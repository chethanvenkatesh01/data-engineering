def add_gcp_connection():
    from airflow.models import Connection, Variable
    import json
    from airflow import settings

    """ Add Airflow connections for GCP default and billing projects """

    # Define OAuth scopes
    scopes = [
        "https://www.googleapis.com/auth/pubsub",
        "https://www.googleapis.com/auth/datastore",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write",
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    # Define connection details
    connections = [
        {
            "conn_id": "google_cloud_default",  # Default connection
            "project_var": "gcp_project",  # Airflow variable for the default project
        },
        {
            "conn_id": "warehouse_connection",  # Billing project connection
            "project_var": "billing_project_id",  # Airflow variable for the billing project
        },
    ]

    # Initialize Airflow session
    session = settings.Session()

    for conn_details in connections:
        conn_id = conn_details["conn_id"]
        project_var = conn_details["project_var"]

        # Get project ID from Airflow variable
        project_id = Variable.get(project_var, default_var=None)
        if not project_id:
            raise ValueError(
                f"Project ID for {project_var} is not set in Airflow Variables"
            )

        # Create Connection object
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="google_cloud_platform",
        )
        conn_extra = {
            "extra__google_cloud_platform__scope": ",".join(scopes),
            "extra__google_cloud_platform__project": project_id,
        }
        conn_extra_json = json.dumps(conn_extra)
        new_conn.set_extra(conn_extra_json)

        # Check if connection already exists
        existing_conn = (
            session.query(Connection)
            .filter(Connection.conn_id == new_conn.conn_id)
            .first()
        )
        if existing_conn:
            session.delete(existing_conn)
            session.commit()
        session.add(new_conn)
        session.commit()
        print(f"GCP connection with `conn_id`={conn_id} created successfully.")


def add_snowflake_connection():
    from airflow.models import Connection, Variable
    from airflow import settings
    import json

    """Add an Airflow connection for Snowflake."""
    new_conn = Connection(
        conn_id="warehouse_connection",
        conn_type="snowflake",
        host=Variable.get("sf_host"),
        schema=Variable.get("sf_schema"),
        login=Variable.get("sf_user"),
        password=Variable.get("sf_password"),
    )

    conn_extra = {
        "account": Variable.get("sf_account"),
        "warehouse": Variable.get("sf_warehouse"),
        "database": Variable.get("sf_database"),
        "role": Variable.get("sf_role"),
    }
    conn_extra_json = json.dumps(conn_extra)
    new_conn.set_extra(conn_extra_json)

    # Save connection to Airflow
    session = settings.Session()
    if (
        not session.query(Connection)
        .filter(Connection.conn_id == new_conn.conn_id)
        .first()
    ):
        session.add(new_conn)
        session.commit()
        print(f"Connection with conn_id='{new_conn.conn_id}' added successfully.")
    else:
        print(f"A connection with conn_id='{new_conn.conn_id}' already exists.")


def add_psg_connection():
    from airflow import models
    from airflow.models import Connection
    import urllib
    from airflow import settings

    """ Add a airflow connection for PSG """
    curr_conn_id = models.Variable.get(
        "postgres_conn_id", default_var="postgres_default"
    )
    # new_conn_id = "postgres_default" + str(int(time()))
    new_conn_id = "postgres_default"
    new_conn = Connection(
        conn_id=new_conn_id,
        conn_type="postgres",
        host=models.Variable.get("PGXHOST"),
        login=models.Variable.get("PGXUSER"),
        port=models.Variable.get("PGXPORT"),
        schema=models.Variable.get("PGXDATABASE"),
        password=urllib.parse.quote(models.Variable.get("PGXPASSWORD")),
    )

    session = settings.Session()
    curr_conn_obj = session.query(Connection).filter(Connection.conn_id == curr_conn_id)
    if curr_conn_obj.first():
        session.delete(curr_conn_obj.first())
        session.commit()
    session.add(new_conn)
    session.commit()
    models.Variable.set("postgres_conn_id", new_conn_id)


def add_warehouse_connection():
    from airflow.models import Variable

    if Variable.get("warehouse") == "SNOWFLAKE":
        add_snowflake_connection()
    else:
        add_gcp_connection()
