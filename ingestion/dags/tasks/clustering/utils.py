import time
import logging
import pandas as pd
from tasks import api_utils, utils
from datetime import datetime
from google.cloud import bigquery
from airflow.models import Variable
import ada.clustering.clustering as sc
from google.cloud.exceptions import NotFound

def generate_auth_token_for_be(target_url:str):
    
    from tasks import api_utils, utils
    """
        Generage authentication token
    """

    token = utils.idtoken_from_metadata_server(target_url)
    print(token)
    return token

def submit_store_clustering_job(
        tenant:str, 
        deploy_env:str, 
        payload:dict
):

    from tasks import api_utils, utils
    import logging
    """
        Submit the store clustering job to the cloud task
    """

    # prepare the inputs for the request
    target_url = f"{api_utils.HTTPS}{tenant}{api_utils.DEPLOY_ENV_MAPPING[deploy_env].value}.{api_utils.DOMAIN_NAME}{api_utils.LSI_CLUSTER_SUBMIT_JOB}"
    payload = {
        "target_url":f"{api_utils.HTTPS}{tenant}{api_utils.DEPLOY_ENV_MAPPING[deploy_env].value}.{api_utils.DOMAIN_NAME}{api_utils.LSI_CLUSTER}",
        "payload":payload
        }
    header = {
        'authorization':generate_auth_token_for_be(target_url)
    }

    # api call
    resp = api_utils.API_methods().post(
        url = target_url,
        payload = payload,
        header=header
    )

    logging.info(f"""
        Submitting store clustering job with 
            payload : {payload}
            response : {resp}
    """)

    return resp['data']


def poll_on_task_ids(successfully_submited_jobs:dict):

    import time
    from tasks import api_utils, utils
    import logging
    """
        Poll on the task id from the table cloud_task_status to check if clustering job ran successfully until timeout.
    """

    # poll on the tasks
    polling_interval = 10
    max_polling_wait_time = 900

    completed_jobs = {}
    failed_jobs = {}
    job_ids_mapping = dict(zip(successfully_submited_jobs.values(), successfully_submited_jobs.keys()))
    
    # poll until timeout
    poll_start_time = time.time()
    while True:

        task_ids_to_poll = list(successfully_submited_jobs.values())
        poll_results = api_utils.poll_on_task_ids(task_ids_to_poll)
        
        # if the task id is absent from the cloud status table then run it manually
        for task_id in task_ids_to_poll:
            try:
                poll_results[task_id]
            except KeyError:
                level_name = job_ids_mapping[task_id]
                logging.info(f"The task id {task_id} for {level_name} was missing from the cloud_task_status table.")
                failed_jobs[level_name] = task_id
                successfully_submited_jobs.pop(level_name)

        # check for the status of the task id
        for task_id in poll_results:

            level_name = job_ids_mapping[task_id]

            if poll_results[task_id] == 'completed':
                completed_jobs[level_name] = task_id
                successfully_submited_jobs.pop(level_name)
                continue

            if poll_results[task_id] == 'aborted':
                logging.info(f"The task id {task_id} for {level_name} was aborted(because of no data).")
                successfully_submited_jobs.pop(level_name)
                continue
            
            if poll_results[task_id] != 'pending':
                logging.info(f"{level_name}[ERROR] :{poll_results[task_id]}")
                failed_jobs[level_name] = task_id
                successfully_submited_jobs.pop(level_name)
        
        # check if the max wait time is exceeded
        poll_end_time = time.time()
        if poll_end_time-poll_start_time > max_polling_wait_time:
            for task_id in poll_results:
                level_name = job_ids_mapping[task_id]
                if poll_results[task_id] != 'completed':
                    logging.info(f"{level_name}[ERROR] :Ran out of time!")
                    failed_jobs[level_name] = task_id
            break
            
        # check if there is atleast one pending task
        if any([poll_results[task_id] == 'pending' for task_id in poll_results]):
            time.sleep(polling_interval)
        else:
            break
    
    return completed_jobs, failed_jobs 


def clustering_locally(query, feature_names, algorithms, no_of_clusters, target_col):
    
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    """
        run store clustering locally for the given df.
    """
    clustering_df = wc.execute_query(query, True)

    # check if the input dataframe is empty := remove that level_name
    if clustering_df.empty:
        return False
    
    # run the clustering
    clustering_results = sc.clustering(
        {
            "df": clustering_df[target_col + feature_names],
            "target": target_col,
            "algorithms": algorithms,
            "n_clusters":no_of_clusters,
            "output_best_result_only":True
        }
    )

    # post processing
    clustering_df = pd.merge(
        left=clustering_df,
        right=clustering_results[target_col + ["labels"]],
        on=target_col,
        how='inner',
        validate=None
    )

    return clustering_df

def local_clustering_results_persistance(df, deploy_env, tenant, clustering_run_id, level_name, summary_table = False):
    
    import logging
    """
        Persist the results of the clustering for the level name that were run locally
    """

    try:

        # save the manual clustering results as well to gcs.
        bucket_name = f'{tenant}-ingestion-{deploy_env}'
        folder_name = 'lsi-clustering'
        if summary_table:
            level_name += "__summary"
        output_table_path = f"gs://{bucket_name}/{folder_name}/{clustering_run_id}/{level_name}.csv"
        df.to_csv(output_table_path)
        return True
    
    except Exception as E:
        logging.info(f"""
            Persisting clustering results for {level_name} failed. Exception : {E}
        """)
        return False
    
def create_clustering_summary_table(level_of_clustering, clustering_df, features):
    
    """
        Genereate clustering summary table from clustering results df
    """

    df_dtypes = clustering_df.dtypes
    feature_agg_mapping = {}
    for feature in features:
        if str(df_dtypes[feature]).startswith(('object', 'category', 'bool')):
            feature_agg_mapping[feature] = pd.Series.mode
        else:
            feature_agg_mapping[feature] = pd.Series.mean
    label_cols = ['cluster_name','labels']
    group_by_cols = level_of_clustering + label_cols
    clustering_summary_df = clustering_df.groupby(group_by_cols).agg(feature_agg_mapping)
    clustering_summary_df.reset_index(inplace=True)

    return clustering_summary_df


def store_clustering_tables_backup(source_table_id):
    
    from google.cloud.exceptions import NotFound
    from google.cloud import bigquery
    import logging
    from datetime import datetime
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    """
        Take the back-up of store_clustering table and store_clustering_summary table with todays name.
    """
    try :
        destination_table_id = source_table_id + "__" + str(datetime.now().date()).replace("-","_")
        wc.copy_table(source_table_id, destination_table_id)
        logging.info(f"Copied the table {source_table_id} as {destination_table_id}")
    except NotFound:
        logging.info(f"This is first time the table {source_table_id} being created.")
    except Exception as e:
        logging.info(f"Failed to copy the table {source_table_id} as {destination_table_id}. Exception : {e}")
        return False
    return True


