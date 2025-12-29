
from airflow import models
import os
from dotenv import load_dotenv

load_dotenv()

class Constants:

    def __init__(self, refresh_mode):
        if refresh_mode == 'local':
                models.Variable.set('fmt_refresh_dataset',os.getenv('ada_GBQ_DATASET'))
                models.Variable.set('fmt_refresh_gcp_project', os.getenv('ada_GCP_PROJECT'))
                models.Variable.set('fmt_refresh_PGXUSER',os.getenv('ada_PGX_USER'))
                models.Variable.set('fmt_refresh_PGXPASSWORD',os.getenv('ada_PGXPASSWORD'))
                models.Variable.set('fmt_refresh_PGXHOST',os.getenv('ada_PGXHOST'))
                models.Variable.set('fmt_refresh_PGXDATABASE',os.getenv('ada_PGXDATABASE'))
                models.Variable.set('fmt_refresh_PGXPORT',os.getenv('ada_PGXPORT'))
                models.Variable.set('fmt_refresh_PGXSCHEMA',os.getenv('ada_PGXSCHEMA'))

        if refresh_mode == 'global':
                models.Variable.set('fmt_refresh_dataset',os.getenv('GBQ_DATASET'))
                models.Variable.set('fmt_refresh_gcp_project', os.getenv('GCP_PROJECT'))
                models.Variable.set('fmt_refresh_PGXUSER',os.getenv('PGX_USER'))
                models.Variable.set('fmt_refresh_PGXPASSWORD',os.getenv('PGXPASSWORD'))
                models.Variable.set('fmt_refresh_PGXHOST',os.getenv('PGXHOST'))
                models.Variable.set('fmt_refresh_PGXDATABASE',os.getenv('PGXDATABASE'))
                models.Variable.set('fmt_refresh_PGXPORT',os.getenv('PGXPORT'))
                models.Variable.set('fmt_refresh_PGXSCHEMA',os.getenv('PGXSCHEMA'))

        models.Variable.set('fmt_refresh_ingestion_type',str(os.getenv('ingestion_type')))
        models.Variable.set('fmt_refresh_fiscal_start_date',str(os.getenv('fiscal_start_date')))
        models.Variable.set('fmt_refresh_aggregation_period',str(os.getenv('aggregation_period')))
        models.Variable.set('fmt_refresh_hierarchies',str(os.getenv('hierarchies')))


class Hierarchies:
	def __init__(self, hierarchies):
		models.Variable.set('fmt_refresh_hierarchies', hierarchies)

class Project:
	def __init__(self):
		models.Variable.set('fmt_refresh_gcp_project',str(os.getenv('GCP_PROJECT')))

project = Project()

fmd_name = 'feature_metadata'

PUB_SUB_TOPIC = "fmd-dag-end-msgs"
PUB_SUB_PROJECT = "ai-ps6060"
PUB_SUB_SUBSCRIPTION = "fmd-dag-end-msgs-sub"