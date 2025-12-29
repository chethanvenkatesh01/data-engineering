class AggregateTable:

    def get_lower_week_threshold(self,table, conn_engine):
        import pandas as pd
        from airflow.models import Variable
        from database_utility.GenericDatabaseConnector import WarehouseConnector

        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)  
        
        """
        Get the lower threshold based on the existing yearweek till which data was aggregated.
        """
        # In case of backfill we are using the .env config filter
        if Variable.get("fmt_backfill", default_var = '') != '':

            # Commenting credentials and using the service account attached to compute engine 
            table_name = wc._get_complete_table_name("fiscal_date_mapping", False)
            delete_weeks = wc.execute_query(
                f"""
                        select min(fiscal_year_week) as fiscal_year_week 
                        from {table_name}
                        where date= current_date()-{Variable.get("fmt_backfill")}
                    """, True)
            Variable.set("lower_week_threshold", delete_weeks.iloc[0][0])
            return delete_weeks.iloc[0][0]
        # Checking the max(fiscal_year_week) in current aggregated_table
        query = f"select max(fiscal_year_week) from {self.pg_schema}.{table}"
        data = pd.read_sql(query, conn_engine)
        Variable.set("lower_week_threshold", data.iloc[0][0])
        return data.iloc[0][0]


    def get_upper_week_bound(self, lower_week_threshold: int = None):
        from airflow.models import Variable
        from datetime import date
        from google.cloud import bigquery
        from database_utility.GenericDatabaseConnector import WarehouseConnector

        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)

        """
        Method to get upper bound for fiscal_year_week till which we need to aggregate
        """
        # Aggregation would be run at the start of every fiscal_year_week and
        # data to be considered is for the past fiscal_year_week

        self.project = Variable.get("gcp_project")
        self.dataset = Variable.get("dataset")
        fiscal_table_name = wc._get_complete_table_name("fiscal_date_mapping", False)
        master_input_table_name = wc._get_complete_table_name("master_input_imputed", False)

        today = str(date.today())
        # In case ingestion type is periodic we would need the lower_week_threshold mandatorily
        if Variable.get("ingestion_type") == "periodic" and not lower_week_threshold and not (eval(Variable.get("fmt_schema_change"))):
            raise Exception(
                f"lower_week_threshold cannot be {lower_week_threshold} when ingestion type is periodic"
            )
        if lower_week_threshold:
            # In case of periodic picke the max week before today's date from fiscal_date_mapping.
            curr_fiscal_week_query = f"""
                select fiscal_year_week from {fiscal_table_name} where 
                date = '{today}'
            """
        else:
            # In case of historic ingestion pick the max week from the master_input_imputed_validated.
            # We cannot rely on fiscal_date_mapping and today's date as the date of historic ingestion
            # and the last date of fiscal_year_week in historic data might differ
            curr_fiscal_week_query = f"""
                select max(fiscal_year_week) from {master_input_table_name} where date > '1990-01-01'
            """
        result = wc.execute_query(curr_fiscal_week_query, True)
        current_fiscal_week = result.iloc[0][0]
        Variable.set("current_fiscal_week", current_fiscal_week)
        if lower_week_threshold:
            # In case of periodic ingestion there will be a lower_week_threshold passed
            week_upper_threshold_query = f"""
                select max(fiscal_year_week) from {fiscal_table_name} where 
                fiscal_year_week > {lower_week_threshold} and fiscal_year_week < {current_fiscal_week}
            """
        else:
            # In case of historic ingestion there will be no lower week threshold
            week_upper_threshold_query = f"""
                select max(fiscal_year_week) from {fiscal_table_name} where 
                fiscal_year_week < {current_fiscal_week}
            """
        result = wc.execute_query(week_upper_threshold_query, True)
        values = result.iloc[0][0]
        Variable.set("upper_week_bound", values)
        return values

    def find_table_names(self):

        import pandas as pd
        from tasks.utils import get_db_engine
        from airflow.models import Variable
        db_engine = get_db_engine()
        mapping_data = pd.read_sql(
            """ select * from global.fmt_mapping
                where active = true
                order by hierarchy_level """,
            db_engine,
        )

        # set the hierarchies level to generate heiracheis code and delete the row
        try:
            Variable.set(
                "hierarchy_code_for_modelling_level",
                mapping_data[mapping_data['tag']=='__hierarchies_level']['unique_by'].to_list()[0]
            )
            mapping_data.drop(mapping_data[mapping_data['tag']=='__hierarchies_level'].index, axis=0, inplace=True)
        except Exception as e:
            print("Warning: The '__hierarchies_level' is not set in fmt_mapping hence skipping the skip level hierarchies code generation.")
            print(f"Err: {e}")
            Variable.set(
                "hierarchy_code_for_modelling_level",
                "[]"                
            )

        Variable.set("fmt_tags",mapping_data.tag.to_list())
        Variable.set("fmt_level",mapping_data.hierarchy_level.to_list())
        Variable.set("fmt_unique_by",mapping_data.unique_by.to_list())

        unique_by_list=mapping_data.unique_by.to_list()
        join_condition=[]
        join_str=[]
        all_hierarchies=set(eval(Variable.get("all_hierarchies")))
        for item in unique_by_list:
            common_cols=all_hierarchies.intersection(set(eval(item)))
            join_str.append(','.join(common_cols))
            join_condition.append(' and '.join([ f"a.{_} = b.{_}" for _ in common_cols]))
        
        Variable.set("fmt_join_str",join_str)
        Variable.set("fmt_join_condition",join_condition)
    
    def get_columns(self, table):
        from airflow.models import Variable
        from database_utility.GenericDatabaseConnector import WarehouseConnector

        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)       
        colnames = wc._get_table_columns(table)
        colnames_str = ', '.join(colnames)
        return colnames_str


    def get_aggregate_query(self, table, num):
        import pandas as pd
        from airflow.models import Variable
        from tasks.utils import year_week_to_date,get_preprocessing_queries, get_db_engine
        from database_utility.GenericDatabaseConnector import WarehouseConnector

        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        """
        Runs the aggregation
        """
        self.pg_schema = Variable.get("PGXSCHEMA")

        db_engine = get_db_engine()
        agg_table_name = f"agg__{table}"

        cols_data = pd.read_sql(
            f"""select concat('{{ ', string_agg(temp,' , ') ,' }}') as col from (select concat( '"',entity,'"',  ' : ' , '"' ,STRING_AGG(gbq_formula, ' , '), '"')  as temp
                                    from {self.pg_schema}.feature_metadata where entity in ('{agg_table_name}') and unique_by is not true and column_name not like '\_\_%%' group by entity)  as a""",
            db_engine,
        )

        special_clause = pd.read_sql(
            f"""select concat('{{ ', string_agg(temp,' , ') ,' }}') as col from (select concat( '"',column_name,'"',  ' : ' , '"' ,STRING_AGG(gbq_formula, ' , '), '"')  as temp
                                    from {self.pg_schema}.feature_metadata where entity in ('{agg_table_name}') and column_name  like '\_\_%%' group by column_name ) as a""",
            db_engine,
        )

        group_by_clause = pd.read_sql(
            f"""select column_name as col from (select column_name
                                    from {self.pg_schema}.feature_metadata where entity in ('{agg_table_name}') and unique_by is true) as a""",
            db_engine,
        )

        cols_dict = eval(cols_data.col.iloc[0].replace("\n", ""))
        special_clause_dict = eval(special_clause.col.iloc[0].replace("\n", ""))
        group_by_cols = group_by_clause.iloc[:, 0].to_list()

        lower_week_threshold = None
        if not (eval(Variable.get("fmt_schema_change"))) and Variable.get("ingestion_type") == "periodic":
            # For fetching the delta to aggregate
            # get fiscal_week > lower_threshold and date <= upper_threshold and
            lower_week_threshold = self.get_lower_week_threshold(
                table=table, conn_engine=db_engine
            )
            Variable.set("fmt_lower_week_threshold",lower_week_threshold)
            lower_week_threshold_date = year_week_to_date(lower_week_threshold, agg_func='min')


        upper_week_bound = self.get_upper_week_bound(lower_week_threshold=lower_week_threshold)
        Variable.set("fmt_upper_week_bound", upper_week_bound)
        upper_week_bound_date = year_week_to_date(upper_week_bound,agg_func='max')

        if special_clause_dict.get("__where_clause"):
            special_clause_dict[
                "__where_clause"
            ] += f" and date <= '{upper_week_bound_date}'"
        else:
            special_clause_dict[
                "__where_clause"
            ] = f" where date <= '{upper_week_bound_date}'"

        if eval(Variable.get("fmt_schema_change")) or Variable.get("ingestion_type") == "non-periodic":
            special_clause_dict[
                "__where_clause"
            ] += f" and date > '1990-01-01'"
            Variable.set("fmt_set_lower_week_for_delete_statement","201001")

        elif Variable.get("fmt_backfill", default_var = '') != '':

            # Commenting credentials and using the service account attached to compute engine 
            table_name = wc._get_complete_table_name("fiscal_date_mapping", False)
            delete_weeks = wc.execute_query(
                f"""
                        select min(fiscal_year_week) as fiscal_year_week, min(date) as date 
                        from {table_name}
                        where fiscal_year_week in (select fiscal_year_week from {table_name} where date= current_date()-{Variable.get("fmt_backfill")} group by 1)
                    """,
                True
            )
            lower_threshold_date = str(delete_weeks.iloc[0,1]).split(" ")[0]
            special_clause_dict[
                "__where_clause"
            ] += f""" and date >= '{lower_threshold_date}'"""
            Variable.set("fmt_set_lower_week_for_delete_statement",delete_weeks['fiscal_year_week'].iloc[0])
            

        elif lower_week_threshold:
            special_clause_dict[
                "__where_clause"
            ] += f" and date >= '{lower_week_threshold_date}'"
            Variable.set("fmt_set_lower_week_for_delete_statement",lower_week_threshold)

        groupby_numbers=[str(_) for _ in range (1,len(group_by_cols)+1)]    

        fmt_table_name = wc._get_complete_table_name(table, False)
        fmt_validated_name = wc._get_complete_table_name(f"{table}_validated_table", False)
        if eval(Variable.get('override_fmt_query')):
            validated_query=get_preprocessing_queries(table,"fmt",eval(Variable.get("fmt_schema_change")))
            if Variable.get("ingestion_type") != 'periodic' or eval(Variable.get("fmt_schema_change")) or eval(Variable.get("model_refresh", default_var="False")):
                query= f"""
                    create or replace table {fmt_table_name} as      
                    (select * from {fmt_validated_name})     
                """
            else:
                cols= self.get_columns(table)
                query = f"""
                    delete from {fmt_table_name} where fiscal_year_week >= (select min(fiscal_year_week) from {fmt_validated_name}); 
                    insert into {fmt_table_name} ({cols})
                    (select {cols} from {fmt_validated_name});
                    """
        else:
            if Variable.get("ingestion_type") != 'periodic' or eval(Variable.get("fmt_schema_change")) or eval(Variable.get("model_refresh", default_var="False")):
                validated_query = f"""
                    create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.{table}_validated_table as
                    (select {','.join(group_by_cols)}, 
                                {cols_dict[agg_table_name]}
                                from {self.project}.{self.dataset}.master_input_imputed as quant_sold_agg 
                                {special_clause_dict.get('__where_clause')}
                                group by {','.join(groupby_numbers)});
                    """
                query= f"""
                    create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.{table} as      
                    (select * from {Variable.get('gcp_project')}.{Variable.get('dataset')}.{table}_validated_table)     
                """
            else:
                cols= self.get_columns(table)
                validated_query= f"""
                    create or replace table {Variable.get('gcp_project')}.{Variable.get('dataset')}.{table}_validated_table
                    as (select {','.join(group_by_cols)}, 
                                {cols_dict[agg_table_name]}
                                from {self.project}.{self.dataset}.master_input_imputed as quant_sold_agg 
                                {special_clause_dict.get('__where_clause')}
                                group by {','.join(groupby_numbers)});
                """
                query = f"""
                    delete from {Variable.get('gcp_project')}.{Variable.get('dataset')}.{table} where fiscal_year_week >= {Variable.get("fmt_set_lower_week_for_delete_statement")}; 
                    insert into {Variable.get('gcp_project')}.{Variable.get('dataset')}.{table} ({cols})
                    (select {cols} from {Variable.get('gcp_project')}.{Variable.get('dataset')}.{table}_validated_table);
                    """
        
        Variable.set(f"aggregation_validated_query_{table}_{num}", validated_query)
        Variable.set(f"aggregation_query_{table}_{num}", query)