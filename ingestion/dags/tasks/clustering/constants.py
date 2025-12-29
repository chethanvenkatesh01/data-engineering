
MASTER_TABLE = 'master_input_imputed'
DEFAULT_WHERE_CONDITION_DATE = "1990-01-01"
DEFAULT_ALGORITHMS = []
DEFAULT_NO_OF_CLUSTERS = []

STORE_CLUSTERING_FMD_INFO_QUERY = """
    select entity as level,
            STRING_AGG(case when column_name not like '\_\_%%' then gbq_formula else null end, ' , ')  as features,
            STRING_AGG(case when column_name not like '\_\_%%' then column_name else null end, ' , ') as feature_names,
            STRING_AGG(case when column_name='__algorithms' then gbq_formula else null end, ' , ') as algorithms,
            STRING_AGG(case when column_name='__where_clause' then gbq_formula else null end, ' , ') as where_clause,
            STRING_AGG(case when column_name='__no_of_clusters' then gbq_formula else null end, ', ') as no_of_clusters
    from global.feature_metadata 
    where entity like 'sc__%%' 
    group by entity"""

ALLOWED_SOURCE_TYPES = ['master_table','sc_cte']