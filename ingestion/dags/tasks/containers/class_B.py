from doctest import master
from airflow import models
from datetime import datetime, timedelta
import os
import pandas as pd
from tasks.factory import DiFactory
from tasks.utils import get_array_columns, get_db_engine
import re
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine
import urllib


class B(DiFactory):
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

        from tasks.utils import get_array_columns, get_db_engine
        from airflow import models
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        from queries.query_builder import merge_query_base_masters
        
        warehouse = models.Variable.get("warehouse")
        warehouse_kwargs = models.Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)

        self.db_engine = get_db_engine()
        
        level = eval(self.level)
        level_str = ",".join(level)
        level_q = []
        for i in level:
            level_q.append(f"'{i}'")

        destination_table_name = wc._get_complete_table_name(self.destination_table, False)
        validated_table_name = wc._get_complete_table_name(self.validated_table, False)

        level_q_const = ",".join(level_q)
        mapping_data = pd.DataFrame.from_dict(
            eval(
                models.Variable.get(
                    f"mapping_data_{self.entity_type}", default_var=0
                ).replace("nan", "None")
            )
        )

        if self.entity_type in self.attribute_hierarchy_list:
            all_hierarchies = eval(models.Variable.get("all_hierarchies"))
            flat_hierarchies_name = []
            for hierarchy in all_hierarchies:
                flat_hierarchies_name.append(
                    f"(product_hierarchies_filter.path ->> '{hierarchy}'::text)::character varying AS {hierarchy}"
                )

            attribute_filter_sp = f"call {models.Variable.get('PGXSCHEMA')}.build_{self.entity_type}_attributes_filter('')"
            hierarchy_filter_sp = (
                f"""call {models.Variable.get('PGXSCHEMA')}.build_{self.entity_type}_hierarchies_filter();
                    call {models.Variable.get('PGXSCHEMA')}.build_{self.entity_type}_hierarchies_filter_flattern();
                """
                if self.entity_type == "product" or self.entity_type == "store"
                else "select 'dummy'"
            )

            attribute_filter_sp = f"call {models.Variable.get('PGXSCHEMA')}.build_{self.entity_type}_attributes_filter('')"

            models.Variable.set(
                f"{self.entity_type}_attributes_filter_sp", attribute_filter_sp
            )
            models.Variable.set(
                f"{self.entity_type}_hierarchies_filter_sp", hierarchy_filter_sp
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
                                    select {master_cols_const} from {validated_table_name}"""
            models.Variable.set(
                f"{self.entity_type}_master_table_query_np_f", master_table_query
            )

            if self.entity_type not in self.attribute_hierarchy_list:
                return True
            else:

                master_cols = mapping_data[
                    mapping_data["required_in_product"] == True
                ].generic_column_name.to_list()
                all_generic_column_names = set(
                    mapping_data.generic_column_name.to_list()
                )
                master_table_columns = wc._get_table_columns(self.destination_table)
                master_table_columns = set(master_table_columns)
                master_table_columns = master_table_columns.intersection(
                    all_generic_column_names
                )

                master_cols_const = ",".join(master_cols)
                master_table_query = f"""call public.sync_{self.destination_table}();"""

                attribute_cols = mapping_data[
                    mapping_data["is_attribute"] == True
                ].generic_column_name.to_list()

                models.Variable.set(
                    f"{self.entity_type}_master_table_query_np", master_table_query
                )

                attribute_table_query = f"""                     
                    call {models.Variable.get('PGXSCHEMA')}.build_{self.entity_type}_attributes();
                """
                models.Variable.set(
                    f"{self.entity_type}_attribute_table_query_np",
                    attribute_table_query,
                )

                all_cols = mapping_data.generic_column_name.to_list()
                season_sub_query = f"""union all SELECT {level_str},'type' as attribute_name, 'season' as attribute_value, season_start_date as start_time, season_end_date as end_time 
                                FROM {models.Variable.get('PGXDATABASE')}.public.{self.validated_table.lower()} 
                                group by {level_str},attribute_name,attribute_value,season_start_date,season_end_date """

                season_query = season_sub_query if "season" in all_cols else ""

                time_attributes_query=f"call global.load_time_attributes('{self.entity_type}_master')"

                models.Variable.set(
                    f"{self.entity_type}_time_attributes_table_query_np",
                    time_attributes_query,
                )

                return True
        else:
            if not (self.entity_type in self.attribute_hierarchy_list):
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
                destination_table_name = wc._get_complete_table_name(self.destination_table, False)
                master_table_query = merge_query_base_masters(destination_table_name, validated_table_name, level_str, master_table_query_f, on_condition_const, 
                                 models.Variable.get(f'optimization_partition_clause_{self.entity_type}'), 
                                 delete_query, master_cols_const_f, source_master_cols_const_f, update_set_const_f, self.pull_type, "B", self.entity_type, self.attribute_hierarchy_list)
                models.Variable.set(
                    f"{self.entity_type}_master_table_query_p", master_table_query
                )

            else:
                master_cols_f = mapping_data[
                    mapping_data["required_in_product"] == True
                ].generic_column_name.to_list()

                attribute_cols = mapping_data[
                    mapping_data["is_attribute"] == True
                ].generic_column_name.to_list()

                master_cols_const_f = ",".join(master_cols_f)
                master_table_query_f = f" (select {master_cols_const_f} from {validated_table_name})"

                array_cols=get_array_columns(self.entity_type)
                source_master_cols_f = []
                update_check_f = []
                update_set_f = []
                for i in master_cols_f:
                    source_master_cols_f.append(f"source.{i}")
                    if i in array_cols:
                        update_check_f.append(f"TO_JSON_STRING (target.{i} )<>TO_JSON_STRING(source.{i})")
                    else:
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

                delete_query = "when not matched by source then update set active=false"
                
                destination_table_name = wc._get_complete_table_name(self.destination_table, False)
                master_query_f = merge_query_base_masters(destination_table_name, validated_table_name, level_str, master_table_query_f, on_condition_const, 
                                 models.Variable.get(f'optimization_partition_clause_{self.entity_type}'), 
                                 delete_query, master_cols_const_f, source_master_cols_const_f, update_set_const_f, self.pull_type, "B", self.entity_type, self.attribute_hierarchy_list)

                models.Variable.set(
                    f"{self.entity_type}_master_table_query_p_f", master_query_f
                )

                master_cols = mapping_data[
                    mapping_data["required_in_product"] == True
                ].generic_column_name.to_list()
                master_cols_const = ",".join(master_cols)

                all_generic_column_names = set(
                    mapping_data.generic_column_name.to_list()
                )

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

                master_table_query = f"""
                call public.sync_{self.destination_table}();
                """

                master_query = master_table_query
                models.Variable.set(
                    f"{self.entity_type}_master_table_query_p", master_query
                )

                attribute_table_query = f""" 
                    call {models.Variable.get('PGXSCHEMA')}.build_{self.entity_type}_attributes_delta();
                                            """

                attribute_query = attribute_table_query
                models.Variable.set(
                    f"{self.entity_type}_attribute_table_query_p", attribute_query
                )


                all_cols = mapping_data.generic_column_name.to_list()
                season_sub_query = f"""union all SELECT {level_str},'type' as attribute_name, 'season' as attribute_value, season_start_date	 as start_time, season_end_date as end_time
                                FROM {models.Variable.get('PGXDATABASE')}.public.{self.validated_table.lower()} 
                                """

                season_query = season_sub_query if "season" in all_cols else ""

                time_attributes_query=f"call global.load_time_attributes('{self.entity_type}_master')"

                models.Variable.set(
                    f"{self.entity_type}_time_attributes_table_query_p",
                    time_attributes_query,
                )
