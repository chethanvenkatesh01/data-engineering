import asyncio
import logging
import pandas as pd
from tasks import api_utils
from airflow.models import Variable
from . import utils as clustering_utils
from . import constants as clustering_const
from tasks.utils import download_csv_files_from_gcs
from tasks.utils import delete_gcs_folder

def get_store_clustering_info_from_fmd_api(tenant, deploy_env):

    from tasks import api_utils
    from airflow.models import Variable
    import logging
    """
        Get the store clustering API to fetch the store clustering info from the FMD
    """
    target_url = f"{api_utils.HTTPS}{tenant}{api_utils.DEPLOY_ENV_MAPPING[deploy_env].value}.{api_utils.DOMAIN_NAME}{api_utils.STORE_CLUSTERING_INFO_FROM_FMD}"
    
    token = clustering_utils.generate_auth_token_for_be(target_url)
    header = {
        'authorization':token
    }

    payload = {
        "project":Variable.get("gcp_project"),
        "dataset":Variable.get("dataset"),
        "level_of_clustering":eval(Variable.get("level_of_store_clustering"))
    }

    resp = api_utils.API_methods().post(
        url = target_url,
        payload = payload,
        header=header
    )

    logging.info(f"""
        Fetch store clustering info from fmd API 
            target_url : {target_url}
            payload : {payload}
            response : {resp}
    """)

    return resp.get('data', False)


def run_store_clustering_through_api(tenant, deploy_env, store_clustering_info, level_of_clustering, main_task_id):

    import logging
    """
        Running clustering by calling store clustering on de-be
    """

    # if the store clustering info is empty from api check locally?
    # submit all the store clustering jobs
    responses = []
    for level_name in store_clustering_info:
        payload = {
            "main_task_id":main_task_id,
            "query":store_clustering_info[level_name]["query"],
            "target_col":store_clustering_info[level_name]["target_col"],
            "feature_names":store_clustering_info[level_name]["feature_names"],
            "algorithms":store_clustering_info[level_name]["algorithms"],
            "no_of_clusters":store_clustering_info[level_name]["no_of_clusters"],
            "level_of_clustering":level_of_clustering,
            "runner":"pipeline"
        }
        
        responses.append(clustering_utils.submit_store_clustering_job(tenant, deploy_env, payload))

    # await the store clustering job responses
    cnt = 0
    successfully_submited_jobs, unsuccessfully_submited_jobs = {}, {}
    for level_name in store_clustering_info:
        resp = responses[cnt]
        if str(resp['status']) == 'True':
            successfully_submited_jobs[level_name] = resp['task_id']
        else:
            unsuccessfully_submited_jobs[level_name] = None
        cnt+=1

    logging.info(f"""
        successfully_submited_jobs : {successfully_submited_jobs},
        unsuccessfully_submited_jobs : {unsuccessfully_submited_jobs}
    """)

    # poll on the task ids
    completed_jobs, failed_jobs = clustering_utils.poll_on_task_ids(successfully_submited_jobs)

    # run the store clustering for both unsuccessfully submited jobs and failed jobs
    jobs_to_run_manually =  unsuccessfully_submited_jobs.update(failed_jobs)

    return completed_jobs, jobs_to_run_manually


def run_store_clustering_locally(jobs_to_run_manually, store_clustering_info, level_of_clustering, deploy_env, tenant, clustering_run_id):

    import logging
    """
        If the store clustering failed to run on some level_names through API, run them locally.
    """

    # run levels manually(if not already run in cloud run)
    jobs_ran_manually = {}
    for level_name in list(jobs_to_run_manually.keys())[:]:
        
        try: 

            logging.info(f"Starting manual clustering job : {level_name}")
            
            # run clustering locally
            query = store_clustering_info[level_name]["query"]
            feature_names = store_clustering_info[level_name]['feature_names']
            algorithms = store_clustering_info[level_name]['algorithms']
            no_of_clusters = store_clustering_info[level_name]['no_of_clusters']
            target_col = store_clustering_info[level_name]['target_col']

            clustering_results = clustering_utils.clustering_locally(query, feature_names, algorithms, no_of_clusters, target_col)
            
            if clustering_results.empty:
                logging.info(f"Couldn't run store clustering locally Either because of missing data. Query - {query}")
                return True
            
            clustering_results['cluster_name'] = clustering_results[level_of_clustering + ['labels']].astype('str').agg('-'.join, axis=1)
            
            # persist clustering results
            _ = clustering_utils.local_clustering_results_persistance(clustering_results, deploy_env, tenant, clustering_run_id, level_name)
            if _:
                jobs_ran_manually[level_name] = True
            
            # create summary table
            clustering_summary_table = clustering_utils.create_clustering_summary_table(level_of_clustering, clustering_results, feature_names)

            # persist clustering summary table
            _ = clustering_utils.local_clustering_results_persistance(clustering_summary_table, deploy_env, tenant, clustering_run_id, level_name, summary_table=True)
            if _:
                logging.info(f"Clustering summary table {level_name} persisted in GCS.")

        except Exception as err:
            logging.info(f"Couldn't run clustering for {level_name} manually. Exception - {err} ")    
            
    return jobs_ran_manually


def clustering_results_persitance_to_gbq(tenant, deploy_env, clustering_run_id, job_names):

    import asyncio
    from tasks.utils import download_csv_files_from_gcs
    from airflow.models import Variable
    import logging
    from tasks.utils import delete_gcs_folder
    """
        Persist the results of the clustering to the gbq.
    """

    # fetch all the gcs results that belong to the main task id
    bucket_name = f'{tenant}-ingestion-{deploy_env}'
    folder_name = 'lsi-clustering'
    
    # add the files persisted by cloud run
    files_to_download = [f"{folder_name}/{clustering_run_id}/{job_name}.csv" for job_name in job_names]
    
    # download the files asynchrounously
    clustered_results = asyncio.run(download_csv_files_from_gcs(bucket_name, files_to_download))

    # combine the resuts in a dataframe
    final_clustered_table = pd.DataFrame()
    for df in clustered_results:
        final_clustered_table = pd.concat(
                            [final_clustered_table,df],
                            axis = 0
                        )
    # post-processing the resultss
    redundent_col_name = 'Unnamed: 0'
    target_col = ['store_code']
    if redundent_col_name in final_clustered_table.columns:
        final_clustered_table.drop([redundent_col_name], axis=1, inplace=True)
    final_clustered_table['labels'] = final_clustered_table['labels'].astype(str)
    # final_clustered_table[target_col]=final_clustered_table[target_col].astype(str)
    final_clustered_table[target_col] = final_clustered_table[target_col].applymap(str)

    final_clustered_table.drop_duplicates(inplace=True)
    
    # persist the results to bigquery
    
    table_id = f"{Variable.get('dataset')}.store_clustering"
    resp = clustering_utils.store_clustering_tables_backup(table_id)
    if not resp:
        logging.warning("Failed to take the back-up of clustering table. The existing table is being replaced.")
    final_clustered_table.to_gbq(table_id, project_id=Variable.get('gcp_project'), if_exists = 'replace')

    # Summary table persisted by cloud run
    files_to_download = [f"{folder_name}/{clustering_run_id}/{job_name}__summary.csv" for job_name in job_names]

    # download the summary files asynchrounously
    clustered_summary_results = asyncio.run(download_csv_files_from_gcs(bucket_name, files_to_download))

    # combine the resuts in a dataframe
    final_clustered_summary_table = pd.DataFrame()
    for df in clustered_summary_results:
        final_clustered_summary_table = pd.concat(
                            [final_clustered_summary_table,df],
                            axis = 0
                        )

    table_id = f"{Variable.get('dataset')}.store_clustering_summary"
    resp = clustering_utils.store_clustering_tables_backup(table_id)
    if not resp:
        logging.warning("Failed to take the back-up of clustering summary table. The existing table is being replaced.")

    final_clustered_summary_table.to_gbq(table_id, project_id=Variable.get('gcp_project'), if_exists = 'replace')

    # delete the results from the gcs bucket
    _ = asyncio.run(delete_gcs_folder(bucket_name,f"{folder_name}/{clustering_run_id}/"))

    return True


def store_clustering():

    from airflow.models import Variable
    from tasks import api_utils
    import logging
    """
        Run store clustering
    """
    tenant = Variable.get('tenant')
    tenant_alias = Variable.get('tenant_alias')
    deploy_env = Variable.get('pipeline')
    level_of_clustering = eval(Variable.get("level_of_store_clustering"))
    
    # fetch the store clustering from fmd using the api
    store_clustering_info = get_store_clustering_info_from_fmd_api(tenant, deploy_env)
    if not store_clustering_info:
        return store_clustering_info
        
    # generate the clustering_unique_key
    clustering_run_id = api_utils.get_unique_id()
    logging.info(f"clustering run id : {clustering_run_id}")
    completed_jobs, jobs_to_run_manually = run_store_clustering_through_api(tenant, deploy_env, store_clustering_info, level_of_clustering, clustering_run_id)

    logging.info(f"""
        completed jobs : {completed_jobs},
        jobs to run manually : {jobs_to_run_manually}    
    """)

    # run clustering locally if the submit-api or clustering api failed
    jobs_ran_manually = {}
    if jobs_to_run_manually:
        jobs_ran_manually = run_store_clustering_locally(jobs_to_run_manually, store_clustering_info, level_of_clustering, deploy_env, tenant_alias, clustering_run_id)

    job_names = list(completed_jobs.values()) + list(jobs_ran_manually.keys())

    # download the clustering job files and persist the result to gbq
    _ = clustering_results_persitance_to_gbq(tenant_alias, deploy_env, clustering_run_id, job_names)

    logging.info("LSI STORE CLUSTERING COMPLETED")

    return True


def sister_store_mapping():
    
    from airflow.models import Variable
    from tasks import api_utils
    import logging
    """
        Call the api to 
    """
    
    tenant = Variable.get('tenant')
    deploy_env = Variable.get('pipeline')
    level_of_clustering = eval(Variable.get("level_of_store_clustering"))

    target_url = f"{api_utils.HTTPS}{tenant}{api_utils.DEPLOY_ENV_MAPPING[deploy_env].value}.{api_utils.DOMAIN_NAME}{api_utils.SISTER_STORE_MAPPING}"
    
    token = clustering_utils.generate_auth_token_for_be(target_url)
    header = {
        'authorization':token
    }

    payload = {
        "project":Variable.get("gcp_project"),
        "dataset":Variable.get("dataset"),
        "level_of_clustering":level_of_clustering
    }

    resp = api_utils.API_methods().post(
        url = target_url,
        payload = payload,
        header=header
    )

    logging.info(f"""
        Fetch store clustering info from fmd API 
            target_url : {target_url}
            payload : {payload}
            response : {resp}
    """)

    return resp['data']


def clustering_branch():
    from airflow.models import Variable
    if Variable.get("model_refresh", "False") == "True" or \
       Variable.get("ingestion_type", "None") == "non-periodic":
        
        return "run_store_clustering"
    else:
        return "sister_store_mapping"
