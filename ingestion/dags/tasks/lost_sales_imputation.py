def store_clustering():
    
    # run store clustering at a week level
    from tasks.utils import (check_if_to_run, delete_gcs_folder,
                             download_csv_files_from_gcs,
                             idtoken_from_metadata_server)
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy import create_engine
    import ada.clustering.clustering as sc
    from sqlalchemy.pool import NullPool
    from tasks import api_utils
    import logging
    import time
    import asyncio
    import urllib
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    check = check_if_to_run("store_clustering",Variable.get("fmt_cadence"))
    if check or (Variable.get("ingestion_type")!="periodic"):
        target_col = ["store_code"]
        deploy_env = Variable.get('pipeline')
        tenant = Variable.get('tenant')
        level_of_clustering = eval(Variable.get("level_of_store_clustering"))

        conn_string = (
            "postgresql://"
            + Variable.get("PGXUSER")
            + ":"
            + urllib.parse.quote(Variable.get("PGXPASSWORD"))
            + "@"
            + Variable.get("PGXHOST")
            + ":"
            + Variable.get("PGXPORT")
            + "/"
            + Variable.get("PGXDATABASE")
        )
        db_engine = create_engine(conn_string, poolclass=NullPool)

        clustering_metadata = pd.read_sql(
            f"""
                select entity as level,
                       STRING_AGG(case when column_name not like '\_\_%%' then gbq_formula else null end, ' , ')  as features,
                       STRING_AGG(case when column_name not like '\_\_%%' then column_name else null end, ' , ') as feature_names,
                       STRING_AGG(case when column_name='__algorithms' then gbq_formula else null end, ' , ') as algorithms,
                       STRING_AGG(case when column_name='__where_clause' then gbq_formula else null end, ' , ') as where_clause,
                       STRING_AGG(case when column_name='__no_of_clusters' then gbq_formula else null end, ', ') as no_of_clusters
                from {Variable.get('PGXSCHEMA')}.feature_metadata 
                where entity like 'sc__%%' 
                group by entity
            """,
            db_engine,
        )
        # get the unique values 
        unique_levels = clustering_metadata["level"].to_list()

        # create the main request id
        main_task_id = api_utils.get_unique_id()
        logging.info(f"Main task id : {main_task_id}")
        # add the clustering jobs to queue
        task_ids = {}
        task_payloads = {}
        unique_levels_to_run_manually = []
        API_methods = api_utils.API_methods()
        for unique_level in unique_levels:

            try :
                logging.info(unique_level)
                # gather the inputs
                clustering_metadata_current_level = clustering_metadata[clustering_metadata['level']==unique_level]
                feature_names = clustering_metadata_current_level['feature_names'].values[0].split(' , ')
                feature_formulas = clustering_metadata_current_level['features'].values[0]
                
                algorithms = clustering_metadata_current_level['algorithms'].values[0]
                if algorithms is not None and algorithms != "":
                    algorithms = eval(algorithms)
                else:
                    algorithms = ["faiss","hdbscan","kmeans", "meanshift", "dbscan", "minibatchkmeans"]

                no_of_clusters = clustering_metadata_current_level['no_of_clusters'].values[0]
                if no_of_clusters is not None and no_of_clusters != "":
                    no_of_clusters = eval(no_of_clusters)
                else:
                    no_of_clusters = list(range(2,12))

                partitioned_column_filter = "date  > '1990-01-01'"
                where_clause = f"""
                    where concat({' , "___" , '.join(level_of_clustering)}) in ('{unique_level.split("__",1)[1]}')
                        and {partitioned_column_filter}
                    """
                where_clause_given = clustering_metadata_current_level['where_clause'].values[0]
                if where_clause_given is not None and where_clause_given != "":
                    where_clause = where_clause + ' and ' + str(where_clause_given)
                clustering_input_table_query = f"""
                    select {','.join(level_of_clustering)},{' , '.join(target_col)},{feature_formulas}
                    from {Variable.get('gcp_project')}.{Variable.get('dataset')}.master_table
                    {where_clause}
                    group by {' , '.join(target_col)},{','.join(level_of_clustering)}
                """
                clustering_input_table_query = clustering_input_table_query.replace("\n"," ")
                clustering_input_table_query = ' '.join(clustering_input_table_query.split())

                # prepare the inputs for the request
                target_url = f"{api_utils.HTTPS}{tenant}{api_utils.DEPLOY_ENV_MAPPING[deploy_env].value}.{api_utils.DOMAIN_NAME}{api_utils.LSI_CLUSTER_SUBMIT_JOB}"
                payload = {
                    "target_url":f"{api_utils.HTTPS}{tenant}{api_utils.DEPLOY_ENV_MAPPING[deploy_env].value}.{api_utils.DOMAIN_NAME}{api_utils.LSI_CLUSTER}",
                    "payload":{
                        "main_task_id":main_task_id,
                        "query":clustering_input_table_query,
                        "target_col":target_col,
                        "features":feature_names,
                        "algorithms":algorithms,
                        "no_of_clusters":no_of_clusters,
                        "level_of_clustering":level_of_clustering,
                        "runner":"pipeline"
                        }
                    }
                task_payloads[unique_level] = payload
                token = idtoken_from_metadata_server(target_url)
                header = {
                    'authorization':token
                }

                logging.info(payload)
                # api call
                resp = API_methods.post(
                    url = target_url,
                    payload = payload,
                    header=header
                )
                if str(resp['data']['status']) == 'True':
                    task_ids[unique_level] = resp['data']['task_id']
                else:
                    unique_levels_to_run_manually.append(unique_level)    

            except Exception as e:
                logging.info(f"{unique_level}[ERROR] :{e}")
                unique_levels_to_run_manually.append(unique_level)
            
        # poll on the tasks
        polling_interval = 10
        task_ids_completed = {}
        task_ids_mapping = dict(zip(task_ids.values(), task_ids.keys()))
        poll_start_time = time.time()
        while True:
            task_ids_to_poll = [task_ids[unique_level] for unique_level in task_ids]
            poll_results = api_utils.poll_on_task_ids(task_ids_to_poll)
            
            # if the task id is absent from the cloud status table then run it manually
            for task_id in task_ids_to_poll:
                try:
                    poll_results[task_id]
                except KeyError:
                    unique_level = task_ids_mapping[task_id]
                    logging.info(f"The task id {task_id} for {unique_level} was missing from the cloud_task_status table.")
                    unique_levels_to_run_manually.append(task_ids_mapping[task_id])
                    task_ids.pop(unique_level)

            for task_id in poll_results:

                unique_level = task_ids_mapping[task_id]

                if poll_results[task_id] == 'completed':
                    task_ids_completed[unique_level] = task_id
                    task_ids.pop(unique_level)
                    continue

                if poll_results[task_id] == 'aborted':
                    task_ids.pop(unique_level)
                    continue
                
                if poll_results[task_id] != 'pending':
                    logging.info(f"{unique_level}[ERROR] :{poll_results[task_id]}")
                    unique_levels_to_run_manually.append(unique_level)
                    task_ids.pop(unique_level)
            
            # check if the max wait time is exceeded
            poll_end_time = time.time()
            max_polling_wait_time = 900
            if poll_end_time-poll_start_time > max_polling_wait_time:
                for task_id in poll_results:
                    unique_level = task_ids_mapping[task_id]
                    if poll_results[task_id] != 'completed':
                        logging.info(f"{unique_level}[ERROR] :Ran out of time!")
                        unique_levels_to_run_manually.append(unique_level)
                break
                
            # check if all there is atleast one pending task
            if any([poll_results[task_id] == 'pending' for task_id in poll_results]):
                time.sleep(polling_interval)
            else:
                break
        
        # run levels manually(if not already run in cloud run)
        for unique_level in unique_levels_to_run_manually[:]:
            
            logging.info(f"Starting manual clustering job : {unique_level}")
            clustering_metadata_current_level = clustering_metadata[clustering_metadata['level']==unique_level]
            
            # gather the inputs for clustering
            clustering_input_table_query = task_payloads[unique_level]['payload']['query']
            clustering_df = wc.execute_query(clustering_input_table_query, True)
            feature_names = task_payloads[unique_level]['payload']['features']
            algorithms = task_payloads[unique_level]['payload']['algorithms']
            no_of_clusters = task_payloads[unique_level]['payload']['no_of_clusters']
            # check if the input dataframe is empty := remove that channel
            if clustering_df.empty:
                unique_levels_to_run_manually.remove(unique_level)
                continue
            
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
            ## post processing
            clustering_df = pd.merge(
                left=clustering_df,
                right=clustering_results[target_col + ["labels"]],
                on=target_col,
            )

            ## create final column
            clustering_df['cluster_name'] = clustering_df[level_of_clustering + ['labels']].astype('str').agg('-'.join, axis=1)

            # save the manual clustering results as well to gcs. #TODO : change the bucket names here
            bucket_name = f'impactsmart-{deploy_env}-ingestion'
            folder_name = 'lsi-clustering'
            output_table_path = f"gs://{bucket_name}/{tenant}/{folder_name}/{main_task_id}/{unique_level}.csv"
            clustering_df.to_csv(output_table_path)

        # fetch all the gcs results that belong to the main task id
        bucket_name = f'impactsmart-{deploy_env}-ingestion'
        folder_name = 'lsi-clustering'
        files_to_download = []
        # add the files persisted by cloud run
        for unique_level in task_ids_completed:
            files_to_download.append(f"{tenant}/{folder_name}/{main_task_id}/{task_ids_completed[unique_level]}.csv")
        # add the files persisted by manual run
        for unique_level in unique_levels_to_run_manually:
            files_to_download.append(f"{tenant}/{folder_name}/{main_task_id}/{unique_level}.csv")
        
        # download the files asynchrounously
            
        string_cols_schema = {col: str for col in level_of_clustering+target_col} 
        clustered_results = asyncio.run(download_csv_files_from_gcs(bucket_name, files_to_download, string_cols_schema))

        # combine the resuts in a dataframe
        final_clustered_table = pd.DataFrame()
        for df in clustered_results:
            final_clustered_table = pd.concat(
                                [final_clustered_table,df],
                                axis = 0
                            )
        # post-processing the resultss
        if 'Unnamed: 0' in final_clustered_table.columns:
            final_clustered_table.drop(['Unnamed: 0'], axis=1, inplace=True)
        final_clustered_table['labels'] = final_clustered_table['labels'].astype(str)
        # final_clustered_table[target_col]=final_clustered_table[target_col].astype(str)
        final_clustered_table[target_col] = final_clustered_table[target_col].applymap(str)
        final_clustered_table.drop_duplicates(inplace=True)
        # persist the resutls to bigquery
        table_id = f"{Variable.get('dataset')}.store_clustering"
        final_clustered_table.to_gbq(table_id, project_id=Variable.get('gcp_project'), if_exists = 'replace')

        # delete the results from the gcs bucket
        _ = asyncio.run(delete_gcs_folder(bucket_name,f"{tenant}/{folder_name}/{main_task_id}/"))
        logging.info("LSI STORE CLUSTERING COMPLETED")

def lsi_materialized_view():
    from airflow.models import Variable
    from queries.query_builder import build_lsi_mv
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    master_table_imputed = wc._get_complete_table_name("master_input_imputed", False)
    master_table = wc._get_complete_table_name("master_table", False)
    lsi_query_mv = build_lsi_mv(master_table_imputed, master_table)
    return lsi_query_mv

def lost_sales_imputation():
    import pandas as pd
    from tasks.utils import (get_impute_features, get_preprocessing_queries)
    from queries.lost_sales import (
        lost_sales_imputation_query,
        price_imputation_query,
    )
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import generate_create_table_ddl_using_select
    from queries.query_builder import build_master_table_transaction

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    if eval(Variable.get("run_lsi_flag", default_var="True")) == False:
        lsi_query_mv = lsi_materialized_view()
        Variable.set("lsi_query_mv", lsi_query_mv)
    else:
        if eval(Variable.get('override_lsi_query')):
            master_table_imputed_name = wc._get_complete_table_name("master_input_imputed", False)
            master_table_imputed_validated_name = wc._get_complete_table_name("master_input_imputed_validated_table", False)
            lsi_query=get_preprocessing_queries("lsi_query","lsi",schema_change=eval(Variable.get("main_master_schema_change")))
            Variable.set("lsi_query_validated", lsi_query)
            master_table_imputed_partition_column = 'date'
            if Variable.get("ingestion_type") != 'periodic' or eval(Variable.get("main_master_schema_change")):
                pi_query_main = generate_create_table_ddl_using_select(
                                    warehouse,
                                    master_table_imputed_name,
                                    f"select * from {master_table_imputed_validated_name}",
                                    partition_by=master_table_imputed_partition_column,
                                )
            else: 
                
                master_table_column_names  = wc._get_table_columns('master_input_imputed')
                master_table_name = wc._get_complete_table_name('master_input_imputed', False)
                master_validated_table_name = wc._get_complete_table_name('master_input_imputed_validated_table', False)
                master_cols_const = ",".join(master_table_column_names)
                backfill = Variable.get("main_master_backfill")
                pi_query_main = build_master_table_transaction(master_table_name, master_validated_table_name, master_cols_const, backfill)
                
        else:
            get_impute_features()
            lsi_query = lost_sales_imputation_query()
            pi_query_validated, pi_query_main = price_imputation_query()
            Variable.set("lsi_query_validated", f"{lsi_query} ; {pi_query_validated}")
        Variable.set("lsi_query_main", pi_query_main)
