import pandas as pd
from airflow import models
from tasks.factory import DiFactory
from tasks.utils import get_db_engine

class A(DiFactory):
    def __init__(
        self,
        entity_type,
        raw_table,
        validated_table,
        attribute_hierarchy_list,
        level,
        destination_table,
        pull_type
    ):
        super().__init__(
            entity_type,
            raw_table,
            validated_table,
            attribute_hierarchy_list,
            level,
            destination_table,
            pull_type
        )

    def create_tables(self):
        from airflow import models
        from tasks.utils import get_db_engine
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        from queries.query_builder import merge_query_base_masters
        
        warehouse = models.Variable.get("warehouse")
        warehouse_kwargs = models.Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)

        destination_table_name = wc._get_complete_table_name(self.destination_table, False)
        validated_table_name = wc._get_complete_table_name(self.validated_table, False)

        level = eval(self.level)
        level_str = ",".join(level)
        level_q = []
        for i in level:
            level_q.append(f"'{i}'")

        mapping_data = pd.DataFrame.from_dict(
            eval(
                models.Variable.get(
                    f"mapping_data_{self.entity_type}", default_var=0
                ).replace("nan", "None")
            )
        )
        if models.Variable.get("ingestion_type") != "periodic":
            master_cols = mapping_data[
                mapping_data["required_in_product"] == True
            ].generic_column_name.to_list()
            partition_columns = mapping_data[mapping_data["is_partition_col"] == True]
            clustering_cols = mapping_data[mapping_data["is_clustering_col"].notnull()].sort_values("is_clustering_col").generic_column_name.to_list()
            assert len(partition_columns) <= 1, "partition_col has more than 1 element."
            assert len(clustering_cols) <= 4, "clustering_cols has more than 4 elements"
            partition_by_clause = ""
            cluster_by_clause = ""
            if len(partition_columns)>0:
                partition_col_name = str(partition_columns.iloc[0].generic_column_name)
                partition_col_type = str(partition_columns.iloc[0].generic_column_datatype).lower()
                if partition_col_type in ['datetime']:
                    partition_by_clause = f"partition by datetime_trunc({partition_col_name}, day)"
                elif partition_col_type in ['timestamp', 'timestamp with time zone', 'timestamp without time zone']:
                    partition_by_clause = f"partition by timestamp_trunc({partition_col_name}, day)"
                elif partition_col_type in ['date']:
                    partition_by_clause = f"partition by {partition_col_name}"
                else:
                    raise Exception(f"Partition column {partition_col_name} has type {partition_col_type}. Currently partitioning on such columns is not supported")
            if len(clustering_cols)>0:
                cluster_by_clause = 'cluster by '+','.join(clustering_cols)
            
            master_cols_const = ",".join(master_cols)
            master_table_query = f""" 
                                    create table {destination_table_name} 
                                     {partition_by_clause} {cluster_by_clause}
                                     as 
                                    select  {master_cols_const} from {validated_table_name}"""
            models.Variable.set(
                f"{self.entity_type}_master_table_query_np_f", master_table_query
            )

            master_table_query_pg = f"""call public.sync_generic_master('{self.destination_table}', '{self.validated_table}');"""
            models.Variable.set(
                f"{self.entity_type}_master_table_query_np_f_pg", master_table_query_pg
            )

        else:
            master_cols_f = mapping_data[
                mapping_data["required_in_product"] == True
            ].generic_column_name.to_list()
            master_cols_const_f = ",".join(master_cols_f)
            validated_table_name = wc._get_complete_table_name(self.validated_table, False)
            master_table_query_f = f" (select {master_cols_const_f} from {validated_table_name})"

            source_master_cols_f = []
            update_check_f = []
            update_set_f = []
            for i in master_cols_f:
                source_master_cols_f.append(f"source.{i}")
                update_check_f.append(f"target.{i}<>source.{i}")
                update_set_f.append((f"target.{i}=source.{i}"))

            source_master_cols_const_f = ",".join(source_master_cols_f)
            update_check_const_f = " or ".join(update_check_f)
            update_set_const_f = ",".join(update_set_f)

            on_condition = []
            for i in level:
                on_condition.append(f"target.{i} = source.{i} ")

            on_condition_const = " and ".join(on_condition)

            insert_source_condition = []
            for i in level:
                insert_source_condition.append(f"source.{i} ")

            delete_query= "when not matched by source then delete" if self.pull_type =="full" else ""
            master_table_query = merge_query_base_masters(destination_table_name, validated_table_name, level_str,  master_table_query_f, on_condition_const, 
                                 models.Variable.get(f'optimization_partition_clause_{self.entity_type}'), 
                                 delete_query, master_cols_const_f, source_master_cols_const_f, update_set_const_f,self.pull_type, "A", self.entity_type, self.attribute_hierarchy_list)
    
            if self.entity_type not in self.attribute_hierarchy_list:

                models.Variable.set(
                    f"{self.entity_type}_master_table_query_p", master_table_query
                )

                all_generic_column_names = set(
                    mapping_data.generic_column_name.to_list()
                )
                self.db_engine = get_db_engine()
                master_table_columns = wc._get_table_columns(self.destination_table)
                master_table_columns = set(master_table_columns)
                master_table_columns = master_table_columns.intersection(
                    all_generic_column_names
                )

                cols_to_be_updated = [
                    f"{_} = excluded.{_} "
                    for _ in master_table_columns
                    if _ != level_str
                ]
                
                master_table_query_pg_p = f"""call public.sync_generic_master('{self.destination_table}', '{self.validated_table}');"""
                models.Variable.set(
                    f"{self.entity_type}_master_table_query_p_f_pg",
                    master_table_query_pg_p,
                )

            else:
                models.Variable.set(
                    f"{self.entity_type}_master_table_query_p_f", master_table_query
                )
