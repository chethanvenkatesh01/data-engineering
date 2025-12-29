import asyncio 
from ada.clustering import clustering as sc
from ada.preprocessing.queries import (store_cluster_query,lost_sales_imputation_query,price_imputation_query)
from common.datastore.datastore import DataStoreFactory
from ada.preprocessing.constants import store_clustering,lost_sales_imputed_table,master_input_imputed

async def lost_sales_imputation(store_clustering_features):
    sc_query= store_cluster_query(store_clustering_features)
    gbq = DataStoreFactory('bigquery')
    store_clustering_base_table = await gbq.run_query(sc_query)
    
    store_clustering_table=sc.clustering(df=store_clustering_base_table,target="store_code")
    store_clustering_table.rename({"labels":"cluster_name"},inplace=True,axis=1)
    await gbq.write_from_df(store_clustering_table,store_clustering) 

    lsi_query= lost_sales_imputation_query()
    await gbq.copy_table(src_table_id=None,src_table_query=lsi_query,dst_table_id=lost_sales_imputed_table) 
    
    pi_query= price_imputation_query()
    await gbq.copy_table(src_table_id=None,src_table_query=pi_query,dst_table_id=master_input_imputed) 
    
    await gbq.delete_tables([lost_sales_imputed_table])
    
    del gbq