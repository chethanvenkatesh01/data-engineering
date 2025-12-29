import argparse
import apache_beam as beam
from beam_nuggets.io import relational_db
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser()
parser.add_argument("--bq_table")
parser.add_argument("--pg_table")
parser.add_argument("--schema")
parser.add_argument("--gcp_project")
parser.add_argument("--dataset")
parser.add_argument("--drivername")
parser.add_argument("--host")
parser.add_argument("--port")
parser.add_argument("--database")
parser.add_argument("--username")
parser.add_argument("--password")
args, beam_args = parser.parse_known_args()
pipeline_options = PipelineOptions(beam_args)
QUERY = f"select * from {args.gcp_project}.{args.dataset}.{args.bq_table}"
source_config = relational_db.SourceConfiguration(
    drivername=str(args.drivername),
    host=args.host,
    port=args.port,
    database=args.database,
    username=args.username,
    password=args.password,
    create_if_missing=True,
)
table_config = relational_db.TableConfiguration(
    name=f"{args.pg_table}", create_if_missing=True
)
with beam.Pipeline(options=pipeline_options) as pipeline:
    event_1 = pipeline | "Read from Bigquery" >> beam.io.ReadFromBigQuery(
        query=QUERY, use_standard_sql=True
    )
    output1 = event_1 | "Writing to Postgres" >> relational_db.Write(
        source_config=source_config, table_config=table_config
    )
