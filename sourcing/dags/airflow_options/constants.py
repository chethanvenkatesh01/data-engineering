import os
from datetime import timedelta
from airflow_options.data_class import DagOptions

dag_options = DagOptions(
    "impact",
    6,
    timedelta(seconds=10),
    False,
    False,
)

list_of_dags = [
    dag.split(".")[0]
    for dag in os.listdir("/opt/airflow/dags/")
    if dag.startswith("dag_")
]

QUERY_SIZE = 600000

client_schema = "int_ia"

# JARS Path
#SFTP_TO_BQ_JAR = f"gs://{Variable.get('gcp_bucket', default_var='sourcing_ingestion')}/dataflow-uber-jars/sftp-to-bigquery-pipeline-1.2.1.jar"
SFTP_TO_BQ_JAR = "sftp-to-bigquery-pipeline-1.8.0.jar"
DB_TO_BQ_JAR = "db-to-bq-pipeline-1.7.0.jar"
GCS_TO_BQ_JAR = "gcs-to-bigquery-pipeline-1.8.0.jar"
SF_TO_BQ_JAR = "snowflake-to-bigquery-pipeline-1.5.5.jar"
SFTP_TO_SF_JAR = "sftp-to-snowflake-pipeline-1.1.0.jar"
DB_TO_SF_JAR = "db-to-sf-pipeline-1.1.0.jar"
GCS_TO_SF_JAR = "gcs-to-snowflake-pipeline-1.1.0.jar"
