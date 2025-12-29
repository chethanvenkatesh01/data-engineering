import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db

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

QUERY = f"select * from {args.database}.{args.schema}.{args.pg_table}"

source_config = relational_db.SourceConfiguration(
    drivername=str(args.drivername),
    host=args.host,
    port=args.port,
    database=args.database,
    username=args.username,
    password=args.password,
)

with beam.Pipeline(options=pipeline_options) as pipeline:
    event_1 = pipeline | "Read from Postgres" >> relational_db.ReadFromDB(
        source_config=source_config, table_name=args.pg_table, query=QUERY
    )

    output1 = event_1 | "Writing to Postgres" >> beam.io.WriteToBigQuery(
        table=args.bq_table,
        dataset=args.dataset,
        project=args.gcp_project,
        write_disposition="WRITE_TRUNCATE",
        schema="SCHEMA_AUTODETECT",
    )
