def find_hierarchy_count(project, dataset):
    from google.cloud import bigquery
    from airflow.models import Variable
    '''
    get the hierarchy count for the client from the product_master
    '''
    query = f"""
            SELECT count(column_name) as hierarchies_count
            FROM {project}.{dataset}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'product_master' and REGEXP_CONTAINS(column_name, r"l[[:digit:]]_name")
    """
    client = bigquery.Client(project=project, location=Variable.get("bigquery_region", default_var="US"))
    job = client.query(query)
    df = job.to_dataframe()

    hierarchies_count = df.iloc[0,0]
    Variable.set('hierarchy_count', hierarchies_count)

class StoreSplitClass:
    def __init__(self, project, dataset, hierarchy_count):
        self.project = project
        self.dataset = dataset
        self.hierarchy_count = int(hierarchy_count)
        self.temp_base_table = 'sp_temp_base'
        self.temp_txn_8weeks_table = 'sp_temp_txn_data_8weeks'
        self.temp_inventory_data_table = 'sp_temp_inventory_data'
        self.temp_base_data_usual_table = 'sp_temp_base_data_usual'
        self.temp_proportions_table = 'sp_temp_proportions'
        self.temp_existing_products_table = 'sp_temp_existing_products'
        self.temp_final_table = 'store_split'

    
    def generate_list(self,upper_limit):
        result = []
        for i in range(self.hierarchy_count):
            s = f"l{i}_name"
            result.append(s)
            if s == upper_limit:
                break
        return result

    def create_base(self):
        from airflow.models import Variable
        '''
        creation of sp_temp_base
        '''
        list_hier = ""
        for i in range(self.hierarchy_count+1):
            list_hier += f"{i+1},"
        list_hier = list_hier[:-1]
        hierarchies = ""
        for i in range(self.hierarchy_count):
            hierarchies+=f'l{i}_name, '
        query = f""" with
                base_store as(
                        select channel ,store_code, store_name, store_type
                        from {self.project}.{self.dataset}.store_master
                        group by 1,2,3,4),

                base_product as (
                        select {hierarchies}
                        product_code
                        from `{self.project}.{self.dataset}.product_master`
                        group by {list_hier}
                ),

                product_store_mapping as (
                        select product_code, store_code, is_active
                        from {self.project}.{self.dataset}.product_store_mapping
                        where is_active is True
                        group by 1,2,3
                ),
                base_1 as (
                        select * from product_store_mapping
                        join base_store
                        using (store_code)
                ),
                base as (
                        select *
                        from base_1
                        join base_product
                        using (product_code)
                )

                select * from base
                """
        Variable.set('base_query', query)
        Variable.set('base_table', self.temp_base_table)

    def create_txn_8weeks(self):
        from airflow.models import Variable
        '''
        latest date for which the transaction is recorded is calculated 
        and transaction details for last 8 weeks is retreived
        '''
        query = f"""with txn_max as(
                Select max(a.date) as max_date , max(c.fiscal_year_week) as max_year_week
                from   `{self.project}.{self.dataset}.transaction_master` a

                join  `{self.project}.{self.dataset}.store_master`
                using(store_code)
                left join `{self.project}.{self.dataset}.fiscal_date_mapping` as c
                using(date)
                where qty > 0
                ),
                txn_data_8weeks as ( -- 8 weeks sales data --
                        select product_code, store_code , channel, sum(Quantity) as sales_art
                        From (
                        Select 1 as channel, product_code, date , transaction_code, store_code, sum(qty ) Quantity
                        from `{self.project}.{self.dataset}.transaction_master`
                        where qty > 0 and date between (Select max_date from txn_max)-55  and (Select max_date from txn_max)
                        Group By 1,2,3,4,5)
                        Group by 1,2,3 )
                select * from txn_data_8weeks
        """
        Variable.set('txn_8weeks_query', query)
        Variable.set('txn_8weeks_table', self.temp_txn_8weeks_table)

    def create_inventory_data(self):
        from airflow.models import Variable
        '''
        latest yearweek for which the transaction is recorded is calculated 
        and inventory details for last 8 weeks is retreived
        '''
        query = f"""with txn_max as(
                Select max(a.date) as max_date , max(c.fiscal_year_week) as max_year_week
                from   `{self.project}.{self.dataset}.transaction_master` a

                join  `{self.project}.{self.dataset}.store_master`
                using(store_code)
                left join `{self.project}.{self.dataset}.fiscal_date_mapping` as c
                using(date)
                where qty > 0
                ),
                inventory_data as (
                        Select im.product_code, im.store_code, Max( im.oh ) oh
                        From `{self.project}.{self.dataset}.inventory_master` im
                        left join `{self.project}.{self.dataset}.fiscal_date_mapping` f_m
                        using(date)
                        where cast(fiscal_year_week as int64) between (Select max_year_week from txn_max)-8  and (Select max_year_week from txn_max)
                        and oh > 0
                        Group by 1,2)
                select * from inventory_data
        """
        Variable.set('inventory_data_query', query)
        Variable.set('inventory_data_table', self.temp_inventory_data_table)

    def create_base_data_usual(self):
        from airflow.models import Variable
        '''
        creation of sp_temp_base_data_usual
        '''    
        def base_join_statement(project, dataset):
            name = "base_overall_data"
            query =    f""" 
            with {name} as 
            (Select a.*, ifnull(sales_art,0)*1  sales_art, ifnull(oh,0) oh 
            from {project}.{dataset}.sp_temp_base a 
            left join 
            {project}.{dataset}.sp_temp_txn_data_8weeks b 
            on a.product_code = b.product_code and a.store_code = b. store_code 
            left join 
            {project}.{dataset}.sp_temp_inventory_data  c 
            on a.product_code = c.product_code and a.store_code = c. store_code),
            """
            return query,name
        def sales_hierarchy_store(project, dataset,hierarchy_level):
            from_query,name = base_join_statement(project, dataset)
            hierarchies = ""
            for i in range(hierarchy_level):
                hierarchies+=f'l{i}_name, '
            query = f""" 
            base_data_usual_store_{hierarchy_level} as (
                    Select channel, {hierarchies} store_code,sum(sales_art) r_h{hierarchy_level}_store 
                    from {name} group by channel,{hierarchies} store_code),
                    """
            return query
        
        def join_base_data_usuals(hierarchy_count):
            query = ""
            hierarchies_list = ""
            r_h_store_list = ""
            for i in range(hierarchy_count):
                    hierarchies_list+=f'l{i}_name, '
                    r_h_store_list+= f'r_h{i+1}_store, '
            query = f"""
                    base_data_usual as(
                        select base_table.*,{r_h_store_list} from 
                        (Select channel, {hierarchies_list}  product_code, store_code,sales_art as sales_art_store 
                        from base_overall_data) base_table
                        inner join
                        """
            query += f"""
                    base_data_usual_store_{hierarchy_count} a
                    using(channel, {hierarchies_list} store_code )
            """
            for hierarchy_level in range(hierarchy_count-1,0,-1):
                hierarchies = ''
                for i in range(hierarchy_level):
                    hierarchies+=f'l{i}_name, '
                query += f"""
                            join base_data_usual_store_{hierarchy_level}
                            using(channel, {hierarchies} store_code)
                        """
                product_level_data_join = f"""
                            join base_data_usual_product_level
                            using(channel, product_code, store_code)
                    """
            query += product_level_data_join +')'
            return query
        
        def sales_hierarchy_product(project, dataset):
            from_query,name = base_join_statement(project, dataset)
            query = f"""
            base_data_usual_product_level as (
                        select channel, product_code,  store_code, sum(sales_art) sales_art 
                        from {name} 
                        group by 1,2,3),"""
            return query
        
        def null_deals(hierarchy_count):
            query = '''select *, '''
            for hierarchy_level in range(hierarchy_count,0,-1):
                query+=f'''sum(r_h{hierarchy_level}_store) over(partition by channel, product_code) r_h{hierarchy_level},'''
            query+='''sum(sales_art_store) over(partition by channel, product_code) as sales_art from (('''

            hierarchies = ''
            for i in range(hierarchy_count):
                hierarchies+=f'l{i}_name, '
            query+=f'''
            select channel,{hierarchies} product_code, store_code, cluster_name,
            case when sales_art_store=0 then avg(case when sales_art_store=0 then null else sales_art_store end) over (partition by  channel, {hierarchies} product_code, cluster_name) else sales_art_store end as sales_art_store,
            '''
            for hierarchy_level in range(hierarchy_count,0,-1):
                hierarchies = ''
                for i in range(hierarchy_level):
                    hierarchies+=f'l{i}_name, ' 
                query+=f'''
                case when r_h{hierarchy_level}_store=0 then avg(case when r_h{hierarchy_level}_store=0 then null else r_h{hierarchy_level}_store end) over (partition by  channel, {hierarchies} cluster_name) else r_h{hierarchy_level}_store end as r_h{hierarchy_level}_store,
                '''
            return query[:-1]

        base_join_query,n = base_join_statement(self.project, self.dataset)
        query = base_join_query
        for i in range(self.hierarchy_count,0,-1):
            query+=sales_hierarchy_store(self.project, self.dataset, i)
        query+=sales_hierarchy_product(self.project, self.dataset)
        query+=join_base_data_usuals(hierarchy_count=self.hierarchy_count)
        query = query + null_deals(self.hierarchy_count)
        query+=f''' from
        (select a.*,b.cluster_name from base_data_usual a
                            left join
                `{self.project}.{self.dataset}.store_clustering` b
                on a.store_code=b.store_code and a.channel=b.channel)))
        '''

        Variable.set('base_data_usual_query', query)
        Variable.set('base_data_usual_table', self.temp_base_data_usual_table)

    def create_proportions(self):
        from airflow.models import Variable
        '''
        creation of sp_temp_proportions
        '''
        safe_divide_query = ""
        store_columns = ""
        hierarchy_columns = ""
        for i in range(self.hierarchy_count):
            safe_divide_query+= f"""ifnull(safe_divide(r_h{i+1}_store,r_h{i+1}),0) as r_store_split_h{i+1}, """
            store_columns+=f'r_h{i+1}_store, '
            hierarchy_columns+=f'r_h{i+1}, '

        query = f"""with proportions as (
                select  *except(sales_art_store, {store_columns} {hierarchy_columns[:-2]} ),
                ifnull(safe_divide(sales_art_store,sales_art),0) as store_split,
                """ + safe_divide_query + f"""from {self.project}.{self.dataset}.sp_temp_base_data_usual) select * from proportions"""

        Variable.set('proportions_query', query)
        Variable.set('proportions_table', self.temp_proportions_table)

    def create_existing_products(self):
        from airflow.models import Variable
        '''
        creation of sp_temp_existing_products
        '''
        hierarchy_list = []
        if Variable.get('store_split_level') == 'product_code':
            hierarchy_list = self.generate_list(f'l{(self.hierarchy_count-1)}_name')
            len_of_hier = len(hierarchy_list)
            hierarchy_list.append('product_code') 
            query = '''
            with existing_products as (
            select *, case when flag_art=1 then store_split
            '''
        else:
            hierarchy_list = self.generate_list(Variable.get('store_split_level'))
            len_of_hier = len(hierarchy_list)
            query = '''
            with existing_products as (
            select *, case
            '''
        hierarchy_list_str =  ",".join(hierarchy_list)
        for i in range(len_of_hier,1,-1):
            query+=f'''when r_flag_l{i}=1 then r_store_split_h{i} '''

        query += f'''
        else r_store_split_h1  end as overall_proportion
        from (
        select *,
        '''
        if Variable.get('store_split_level') == 'product_code':
            query+= f'''
            case when count((case when store_split is null or store_split=0 then 1 end)) over (partition by {hierarchy_list_str},channel)/count(case when store_split is null or store_split=0 then 1 else 1 end) over (partition by {hierarchy_list_str},channel)>0.05 then 0 else 1 end  flag_art,
        '''

        for i in range(len_of_hier,1,-1):
            query+=f'''
            case when count((case when r_store_split_h{i} is null or r_store_split_h{i}=0 then 1 end)) over (partition by {hierarchy_list_str},channel)/count((case when r_store_split_h{i} is null or r_store_split_h{i}=0 then 1 else 1 end)) over (partition by {hierarchy_list_str},channel)>0.05 then 0 else 1 end  r_flag_l{i},
            '''

        query = query[:-1]

        query+=f'''
        from {self.project}.{self.dataset}.sp_temp_proportions))
        select *except(overall_proportion),coalesce(overall_proportion,0) as overall_proportion from existing_products
        '''
        Variable.set('existing_products_query', query)
        Variable.set('existing_products_table', self.temp_existing_products_table)


    def create_final_table(self):
        from airflow.models import Variable
        '''
        creation of store_split table
        '''
        hierarchy_list = []
        if Variable.get('store_split_level') == 'product_code':
            hierarchy_list = self.generate_list(f'l{(self.hierarchy_count-1)}_name')
            hierarchy_list.append('product_code') 
            hierarchy_list_str =  ",".join(hierarchy_list)
            join_list_str = hierarchy_list_str
        else:
            hierarchy_list = self.generate_list(Variable.get('store_split_level'))        
            hierarchy_list_str =  ",".join(hierarchy_list)
            join_list_str = hierarchy_list_str + ',product_code'

        level_num = len(hierarchy_list) + 1
        query = f"""WITH final_table AS (
                SELECT *,SAFE_DIVIDE(contribution, SUM(contribution) OVER(PARTITION BY {hierarchy_list_str},channel)) AS normalized_contribution
                FROM (SELECT {hierarchy_list_str},store_code,channel,SUM(overall_proportion) AS contribution FROM {self.project}.{self.dataset}.sp_temp_existing_products
                GROUP BY {hierarchy_list_str},store_code,channel)
                INNER JOIN (SELECT {join_list_str},hierarchy_code 
                FROM {self.project}.{self.dataset}.product_hierarchies_filter_flattened
                WHERE level = {level_num}) hc
                USING({hierarchy_list_str})
        )

        SELECT store_code,channel,product_code,hierarchy_code,case when round(sum(normalized_contribution) over (partition by {hierarchy_list_str},channel))!=1 or 
        round(sum(normalized_contribution) over (partition by {hierarchy_list_str},channel)) is null then 1/count(*) over (partition by {hierarchy_list_str},channel) else normalized_contribution end as contribution
        FROM
        final_table

        """
        Variable.set('final_table_query', query)
        Variable.set('final_table', self.temp_final_table)


# from airflow.models import Variable


# class StoreSplitClass:
#     """
#     This class implements logic for store split for a given hierarchy level.
#     Output is 2 tables in GBQ
#     1. hier-sku-store-contri level
#     2. hier-store-contri level # to be used by pre-season non-core entities
#     """

#     def __init__(self, gbq_dataset, store_split_level):
#         """
#         Params:
#         gbq_dataset : Dataset in GBQ eg.Ada_Test
#         store_split_level : hierarchy level name eg. l0_name,l1_name
#         """
#         self.gbq_dataset = gbq_dataset
#         self.source_table = "master_input_imputed"
#         self.store_split_level = store_split_level 
#         self.temp_store_contri_table_hier = (
#             f"Temp_Store_Contri_{self.store_split_level}_hier"
#         )

#         self.temp_store_contri_table_sku = (
#             f"Temp_Store_Contri_{self.store_split_level}_sku"
#         )

#         self.filtered_sku_store_table = f"{self.store_split_level}_filtered_sku_stores"
#         self.filtered_hier_store_table = f"{self.store_split_level}_filtered_hier_stores"
#         self.store_split_results_sku = f"Store_Split_Results_{self.store_split_level}_sku"
#         self.store_split_results_hier = (
#             f"Store_Split_Results_{self.store_split_level}_hier"
#         )
#         self.fiscal_calendar = "fiscal_master"



#     def calculate_store_contri_hier(self):
#         """
#         store_contri = Store_Quantity_at_Hierarchy_code_level/Hierarchy_code_total_quantity (for last 13 weeks)
#         if some hierarchy doesn't fall under 13 weeks, calculate with last 1 year
#         """

#         with_hier_store_sum_table = f"{self.store_split_level}_store_sum"
#         with_hier_sum_table = f"{self.store_split_level}_sum"

#         query1 = (
#             f"WITH {with_hier_store_sum_table} AS (SELECT {self.store_split_level}, store_code,SUM(qty) as Store_Quantity"
#             f" FROM  (select * from {self.gbq_dataset}.{self.source_table}) "
#             f" WHERE fiscal_year_week in (select fiscal_year_week from {self.gbq_dataset}.{self.fiscal_calendar} "
#             f" WHERE fiscal_year_week<=( SELECT max(fiscal_year_week) from {self.gbq_dataset}.{self.source_table})"
#             f" GROUP BY 1 ORDER BY fiscal_year_week desc LIMIT 13)  and qty>0 GROUP BY 1,2)"
#         )

#         query2 = (
#             f" {with_hier_sum_table} AS (SELECT {self.store_split_level}, SUM(qty) as {self.store_split_level}_Quantity"
#             f" FROM (select * from {self.gbq_dataset}.{self.source_table})"
#             f" WHERE fiscal_year_week in (select fiscal_year_week from {self.gbq_dataset}.{self.fiscal_calendar} "
#             f" WHERE fiscal_year_week<=( SELECT max(fiscal_year_week) from {self.gbq_dataset}.{self.source_table})"
#             f" GROUP BY 1 ORDER BY fiscal_year_week desc LIMIT 13) and qty>0 GROUP BY 1)"
#         )

#         query = (
#             f"{query1} , {query2 } SELECT *, ifnull(SAFE_DIVIDE(Store_Quantity,{self.store_split_level}_Quantity),0) as Contribution"
#             f" FROM {with_hier_store_sum_table} INNER JOIN {with_hier_sum_table} as b USING({self.store_split_level})"
#         )

#         Variable.set('calculate_store_contri_hier_query', query)
#         Variable.set('calculate_store_contri_hier_table', self.temp_store_contri_table_hier)

#     # ------------------------------ for hierarchies missing in 13 weeks ------------------------------------

#     def filter_one_year(self):
#         """
#         Filter on basis of last 1 year data
#         """
#         with_hier_store_sum_table = f"{self.store_split_level}_store_sum"
#         with_hier_sum_table = f"{self.store_split_level}_sum"
#         with_missing_hier_store_sum_table = f"missing_{with_hier_store_sum_table}"
#         with_missing_hier_sum_table = f"missing_{with_hier_sum_table}"

#         query1_for_missing_hiers = (
#             f"WITH {with_missing_hier_store_sum_table} AS (SELECT {self.store_split_level}, store_code,SUM(qty) as Store_Quantity"
#             f" FROM   {self.gbq_dataset}.{self.source_table} "
#             f" WHERE {self.store_split_level} not in ( Select {self.store_split_level} from  {self.gbq_dataset}.{self.temp_store_contri_table_hier})"
#             f" and fiscal_year_week in (select fiscal_year_week from {self.gbq_dataset}.{self.fiscal_calendar} "
#             f" WHERE fiscal_year_week<=( SELECT max(fiscal_year_week) from {self.gbq_dataset}.{self.source_table})"
#             f" GROUP BY 1 ORDER BY fiscal_year_week desc LIMIT 52)  and qty>0 GROUP BY 1,2)"
#         )

#         query2_for_missing_hiers = (
#             f" {with_missing_hier_sum_table} AS (SELECT {self.store_split_level}, SUM(qty) as {self.store_split_level}_Quantity"
#             f" FROM  {self.gbq_dataset}.{self.source_table} "
#             f" WHERE  {self.store_split_level} not in ( Select {self.store_split_level} from  {self.gbq_dataset}.{self.temp_store_contri_table_hier})"
#             f" and fiscal_year_week in (select fiscal_year_week from {self.gbq_dataset}.{self.fiscal_calendar} "
#             f" WHERE fiscal_year_week<=( SELECT max(fiscal_year_week) from {self.gbq_dataset}.{self.source_table})"
#             f" GROUP BY 1 ORDER BY fiscal_year_week desc LIMIT 52)  and qty>0 GROUP BY 1)"
#         )

#         query = (
#             f"{query1_for_missing_hiers} , {query2_for_missing_hiers } SELECT *, ifnull(SAFE_DIVIDE(Store_Quantity,{self.store_split_level}_Quantity),0) as Contribution"
#             f" FROM {with_missing_hier_store_sum_table} INNER JOIN {with_missing_hier_sum_table} USING({self.store_split_level})"
#         )

#         Variable.set('filter_one_year_query', query)
#         Variable.set('filter_one_year_table', self.temp_store_contri_table_hier)


#     def calculate_store_contri_sku(self):
#         # ------------------------------- bringing down to hier-sku-store-contri level -------------------------------
#         """
#         Inner join with master table
#         """

#         query_to_join_hier_with_skus = (
#             f" Select a.{self.store_split_level},a.store_code, b.product_code ,a.Contribution FROM {self.gbq_dataset}.{self.temp_store_contri_table_hier} as a INNER JOIN"
#             f" ( Select {self.store_split_level}, product_code from {self.gbq_dataset}.{self.source_table} GROUP BY 1,2) as b USING ({self.store_split_level}) "
#         )

#         Variable.set('calculate_store_contri_sku_query', query_to_join_hier_with_skus)
#         Variable.set('calculate_store_contri_sku_table', self.temp_store_contri_table_sku)

#     def filter_stores(self):
#         """
#         Select stores where inventory for last 1 year has been non zero for at least 1 week
#         """
        
#         with_sku_store_filtered_table = f"{self.store_split_level}_filter_sku_stores"
#         with_hier_store_filtered_table = f"{self.store_split_level}_filter_hier_stores"

#         sku_store_keep_query = (
#             f"WITH {with_sku_store_filtered_table} as (SELECT product_code ,{self.store_split_level}, store_code FROM  {self.gbq_dataset}.{self.source_table} "
#             f" WHERE oh > 0 and fiscal_year_week in (select fiscal_year_week from {self.gbq_dataset}.{self.fiscal_calendar} "
#             f" WHERE fiscal_year_week<=( SELECT max(fiscal_year_week) from {self.gbq_dataset}.{self.source_table})"
#             f" GROUP BY 1 ORDER BY fiscal_year_week desc LIMIT 52) GROUP BY 1,2,3)"
#         )

#         query1 = (
#             f"{sku_store_keep_query} SELECT a.* FROM  {self.gbq_dataset}.{self.temp_store_contri_table_sku} as a INNER JOIN {with_sku_store_filtered_table} as b "
#             f" USING({self.store_split_level},product_code,store_code) "
#         )

#         hier_store_keep_query = (
#             f"WITH {with_hier_store_filtered_table} as (SELECT {self.store_split_level}, store_code FROM  {self.gbq_dataset}.{self.source_table} "
#             f" WHERE oh > 0 and fiscal_year_week in (select fiscal_year_week from {self.gbq_dataset}.{self.fiscal_calendar} "
#             f" WHERE fiscal_year_week<=( SELECT max(fiscal_year_week) from {self.gbq_dataset}.{self.source_table})"
#             f" GROUP BY 1 ORDER BY fiscal_year_week desc LIMIT 52) GROUP BY 1,2)"
#         )

#         query2 = (
#             f"{hier_store_keep_query} SELECT a.* FROM  {self.gbq_dataset}.{self.temp_store_contri_table_hier} as a INNER JOIN {with_hier_store_filtered_table} as b "
#             f" USING({self.store_split_level}, store_code) "
#         )

#         Variable.set('filtered_sku_store_query', query1)
#         Variable.set('filtered_hier_store_query', query2)
#         Variable.set('filtered_sku_store_table', self.filtered_sku_store_table)
#         Variable.set('filtered_hier_store_table', self.filtered_hier_store_table)

#     def normalize_store_contri(self):
#         """
#         Results are at product_code,store_code,year_week level
#         """

#         with_summed_sku_table = (
#             f"Filtered_Store_Contri_{self.store_split_level}_Summed_prod"
#         )

#         with_summed_hier_table = (
#             f"Filtered_Store_Contri_{self.store_split_level}_Summed_hier"
#         )

#         normalize_query_sku = (
#             f"With {with_summed_sku_table} as "
#             f" (SELECT {self.store_split_level},product_code,sum(Contribution) as contribution_sum from {self.gbq_dataset}.{self.filtered_sku_store_table}"
#             f" group by {self.store_split_level},product_code )"
#             f" SELECT {self.store_split_level},product_code,store_code, Contribution/contribution_sum as contribution"
#             f" from {with_summed_sku_table} inner join {self.gbq_dataset}.{self.filtered_sku_store_table} using ({self.store_split_level},product_code)  "
#         )

#         normalize_query_hier = (
#             f"With {with_summed_hier_table} as "
#             f" (SELECT {self.store_split_level},sum(Contribution) as contribution_sum from {self.gbq_dataset}.{self.filtered_hier_store_table}"
#             f" group by {self.store_split_level} )"
#             f" SELECT {self.store_split_level},store_code, Contribution/contribution_sum as contribution"
#             f" from {with_summed_hier_table} inner join {self.gbq_dataset}.{self.filtered_hier_store_table} using({self.store_split_level})  "
#         )


#         Variable.set('normalize_query_sku', normalize_query_sku)
#         Variable.set('normalize_query_hier', normalize_query_hier)
#         Variable.set('normalize_sku_table', self.store_split_results_sku)
#         Variable.set('normalize_hier_table', self.store_split_results_hier)
