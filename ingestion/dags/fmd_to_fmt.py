
# from airflow import DAG
# from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
# from pathlib import Path
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python import PythonOperator
# from airflow import models
# from datetime import datetime,timedelta
# import constants.constant_fmd_to_fmt as constant
# from tasks.aggregate_table_fmd_fmt_refresh import get_aggregate_query
# from tasks.drop_pg_table import drop_table
# from tasks.copy_gbq_to_pg import copy_gbq_to_pg
# import psycopg2
# from google.cloud import pubsub_v1
# import json


# BASE_DIR = Path(__file__).resolve(strict=True).parent
# AGG_FILE= BASE_DIR / 'copy_aggregated_tables_fmd_fmt.py'

# def env_var(**context):
#         refresh_mode = context['dag_run'].conf['refresh_mode']
#         hierarchies = context['dag_run'].conf['hierarchies']
#         constant.Constants(refresh_mode)
#         constant.Hierarchies(hierarchies)

# def insert_notification():
#     PUB_SUB_TOPIC = constant.PUB_SUB_TOPIC
#     PUB_SUB_PROJECT = constant.PUB_SUB_PROJECT

#     project = PUB_SUB_PROJECT
#     topic = PUB_SUB_TOPIC
#     payload = {'msg':'fmt is created in db'}


#     publisher = pubsub_v1.PublisherClient()
#     topic_path = publisher.topic_path(project, topic)
#     data = json.dumps(payload).encode("utf-8")
#     future = publisher.publish(
#         topic_path, data
#     )
#     print(future.result())

# def call_back_if_dag_fails(context):
#     def push_error_notification_pubsub():
#         PUB_SUB_TOPIC = constant.PUB_SUB_TOPIC
#         PUB_SUB_PROJECT = constant.PUB_SUB_PROJECT

#         project = PUB_SUB_PROJECT
#         topic = PUB_SUB_TOPIC
#         payload = {'msg':'error while creating fmt in db'}


#         publisher = pubsub_v1.PublisherClient()
#         topic_path = publisher.topic_path(project, topic)
#         data = json.dumps(payload).encode("utf-8")
#         future = publisher.publish(
#             topic_path, data
#         )
#         print(future.result())

#     refresh_mode = context['dag_run'].conf['refresh_mode']
#     if refresh_mode == 'global':
#         user = models.Variable.get('fmt_refresh_PGXUSER', default_var = '')
#         password = models.Variable.get('fmt_refresh_PGXPASSWORD', default_var = '')
#         host = models.Variable.get('fmt_refresh_PGXHOST', default_var = '')
#         port = models.Variable.get('fmt_refresh_PGXPORT', default_var = '')
#         database = models.Variable.get('fmt_refresh_PGXDATABASE', default_var = '')
#         schema = models.Variable.get('fmt_refresh_PGXSCHEMA', default_var = '')
#         connection = psycopg2.connect(user=user,
#                                     password=password,
#                                     host=host,
#                                     port=port,
#                                     database=database)
#         cursor = connection.cursor()
#         query_revert = f"""
#         DELETE FROM {schema}.{constant.fmd_name};
#         INSERT INTO {schema}.{constant.fmd_name} SELECT * FROM {schema}.{constant.fmd_name}_backup
#         """

#         cursor.execute(query_revert)
#         connection.commit()

#         if connection:
#             cursor.close()
#             connection.close()

#     elif refresh_mode == 'local':
#         push_error_notification_pubsub()

# def call_back_if_dag_runs_successfully(context):
#     refresh_mode = context['dag_run'].conf['refresh_mode']
#     if refresh_mode == 'global':
#         user = models.Variable.get('fmt_refresh_PGXUSER', default_var = '')
#         password = models.Variable.get('fmt_refresh_PGXPASSWORD', default_var = '')
#         host = models.Variable.get('fmt_refresh_PGXHOST', default_var = '')
#         port = models.Variable.get('fmt_refresh_PGXPORT', default_var = '')
#         database = models.Variable.get('fmt_refresh_PGXDATABASE', default_var = '')
#         schema = models.Variable.get('fmt_refresh_PGXSCHEMA')
#         connection = psycopg2.connect(user=user,
#                                     password=password,
#                                     host=host,
#                                     port=port,
#                                     database=database)
#         cursor = connection.cursor()
#         query_update_backup_fmd = f"""
#         DROP TABLE IF EXISTS {schema}.{constant.fmd_name}_backup;
#         """

#         cursor.execute(query_update_backup_fmd)
#         connection.commit()

#         if connection:
#             cursor.close()
#             connection.close()

#     elif refresh_mode == 'local':
#         insert_notification()



# default_args = {
#     'owner': 'impact',
#     'retries': 4,
#     'retry_delay': timedelta(minutes=3),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'provide_context': True,
#     'project_id': models.Variable.get('fmt_refresh_gcp_project', default_var = '')
# }



# with DAG(
#     dag_id='fmd_to_fmt',
#     default_args =default_args,
#     schedule_interval=None,
#     catchup=False,
#     max_active_tasks = 4,
#     max_active_runs = 1,
#     start_date=datetime(2021,1,1)) as dag:

#     define_env_var = PythonOperator(
#         task_id=f'define_env_variables',
#         python_callable =env_var,
#         on_failure_callback = call_back_if_dag_fails
#     )


#     hierarchies = eval(models.Variable.get('fmt_refresh_hierarchies',default_var="['dummy']"))
#     task_aggregation = 0

#     update_fmd_backup = DummyOperator(
#         task_id='update_feature_metadata_backup_when_dag_is_success',
#         trigger_rule='all_success',
# 	    on_success_callback = call_back_if_dag_runs_successfully
#     )

#     for i, hierarchy in enumerate(hierarchies):

#         set_query = PythonOperator(
#             task_id = f"create_query_for_FMT_{hierarchy}",
#             python_callable = get_aggregate_query,
#                     op_kwargs = {
#                         'hierarchies': hierarchies[:i+1] if hierarchy != 'product_code' else hierarchies[-1:],
#                         'num': i},
#             on_failure_callback = call_back_if_dag_fails
#         )

#         create_fmt = BigQueryOperator(
#             task_id =  f'create_aggregate_table_{hierarchy}',
#             use_legacy_sql = False,
#             write_disposition = 'WRITE_TRUNCATE',
#             sql = models.Variable.get(f'fmt_refresh_aggregation_query_{i}', default_var=''),
#             destination_dataset_table = f"{models.Variable.get('fmt_refresh_gcp_project')}.{models.Variable.get('fmt_refresh_dataset', default_var = '')}.master_aggregate_{hierarchy}",
#             on_failure_callback = call_back_if_dag_fails
#         )
#         copy_gbq_to_pg_task = PythonOperator(
#             task_id = f"copy_table_master_aggregate_{hierarchy}_gbq_to_postgres",
#             python_callable = copy_gbq_to_pg,
#                     op_kwargs = {
#                         'hierarchy': hierarchy
#                         },
#             on_failure_callback = call_back_if_dag_fails
#         )

#         drop_duplicate_table = PythonOperator(
#             task_id=f'drop_duplictae_agg_{hierarchy}_table_in_postgress',
#             python_callable =drop_table,
#             op_kwargs={'hierarchy': hierarchy},
#             on_failure_callback = call_back_if_dag_fails
#         )

#         define_env_var >> set_query >> create_fmt >> copy_gbq_to_pg_task >> drop_duplicate_table >> update_fmd_backup