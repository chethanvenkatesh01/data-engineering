from abc import ABC, abstractmethod

class DiFactory(ABC):
    """
    Factory class for Data Ingestion Validation module.

    """

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
        self.entity_type = entity_type
        self.raw_table = raw_table
        self.validated_table = validated_table
        self.attribute_hierarchy_list = attribute_hierarchy_list
        self.level = level
        self.destination_table = destination_table
        self.pull_type=pull_type

    def transform_validate(self):
        import pandas as pd
        from airflow import models
        from constants.constant import database_to_warehouse_dtypes, generate_cast_string
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        from queries.query_builder import build_transform_validate_query

        warehouse = models.Variable.get("warehouse")
        db_to_wh_dtypes = database_to_warehouse_dtypes(warehouse)
        mapping_data = pd.DataFrame.from_dict(
            eval(
                models.Variable.get(
                    f"mapping_data_{self.entity_type}", default_var=0
                ).replace(": nan", ": None")
            )
        )
        formula_cols = (
            mapping_data[mapping_data.formula.notnull()]
            .formula.str.replace('"', "")
            .to_list()
        )
        level_cols = mapping_data[
            mapping_data["unique_by"] == True
        ].generic_column_name.to_list()
        except_cols = mapping_data[
            (
                (mapping_data.source_column_name.notnull())
                & (mapping_data.formula.notnull())
            )
        ].source_column_name.to_list()
        # Below code is added to remove conflicts where source data and generic data
        # have same name and source_column_name is null and we use source column in formula
        # example active flag is set using Active=1 formula. In this case Active needs to be in except list

        generic_formula_cols = mapping_data[
            mapping_data.formula.notnull()
        ].generic_column_name.to_list()
        formula_cols_data_type=mapping_data[
            mapping_data.formula.notnull()
        ].generic_column_datatype.to_list()

        old_column_names = mapping_data[
            (
                (mapping_data.source_column_name.notnull())
                & (mapping_data.formula.isnull())
            )
        ].source_column_name.to_list()
        generic_cols = mapping_data[
            (
                (mapping_data.source_column_name.notnull())
                & (mapping_data.formula.isnull())
            )
        ].generic_column_name.to_list()
        generic_data_type = mapping_data[
            (
                (mapping_data.source_column_name.notnull())
                & (mapping_data.formula.isnull())
            )
        ].generic_column_datatype.to_list()
        
        old_column_names_sorted = old_column_names.copy()
        old_column_names_sorted = old_column_names_sorted.sort()
        null_check = mapping_data[
                (mapping_data["is_null_allowed"] == False)
        ].generic_column_name.to_list()
        hierarchies = mapping_data[mapping_data["is_hierarchy"] == True ].sort_values("hierarchy_level").generic_column_name.to_list()
        models.Variable.set(f"{self.entity_type}_hierarchy_list", hierarchies)

        null_valued_columns = mapping_data[
            (
                (mapping_data.source_column_name.isnull())
                & (mapping_data.formula.isnull())
            )
        ].generic_column_name.to_list()
        temp_null_valued_cols = []
        # for i in null_valued_columns:
        #     temp_null_valued_cols.append(f"null as {i}")

        null_valued_cols_data_type = mapping_data[(mapping_data.source_column_name.isnull()) & (mapping_data.formula.isnull())].generic_column_datatype.to_list()
        for i, col_name in enumerate(null_valued_columns):
            col_datatype = null_valued_cols_data_type[i]
            try:
                wh_datatype = db_to_wh_dtypes[col_datatype.lower()]
                if 'array' in wh_datatype.lower():
                    element_type = wh_datatype.split('<')[-1].split('>')[0]
                    temp_null_valued_cols.append(f"cast(null as ARRAY<{element_type}>) as {col_name}")
                else:
                    temp_null_valued_cols.append(f"cast(null as {wh_datatype}) as {col_name}")
            except KeyError:
                temp_null_valued_cols.append(f"cast(null as STRING) as {col_name}")

        temp_null_valued_cols_const = ",".join(temp_null_valued_cols)

        formula_cols_data_type_bq = []
        for i in formula_cols_data_type:
            try:
                formula_cols_data_type_bq.append(db_to_wh_dtypes[i])
            except ValueError:
                formula_cols_data_type_bq.append("int64")

        transformed_cols = []
        for i in range(0, len(formula_cols)):
            formula_safe_cast_str = generate_cast_string(warehouse,formula_cols_data_type_bq[i],formula_cols[i], formula_cols_data_type_bq[i], generic_formula_cols[i])
            transformed_cols.append(formula_safe_cast_str)
        transformed_cols_const = ",".join(transformed_cols)
        old_cols = ",".join(list(set(old_column_names)))

        generic_data_type_bq = []
        for i in generic_data_type:
            try:
                generic_data_type_bq.append(db_to_wh_dtypes[i])
            except ValueError:
                generic_data_type_bq.append("int64")

        typecast_cols = []
        for i in range(0, len(old_column_names)):
            generic_safe_cast_str = generate_cast_string(warehouse, generic_data_type_bq[i], old_column_names[i], generic_data_type_bq[i], generic_cols[i])
            typecast_cols.append(generic_safe_cast_str)

        typecast_cols_const = ",".join(typecast_cols)
        null_cols = []
        for i in null_check:
            null_cols.append(f"{i} is null ")

        null_cols_const = "or ".join(null_cols)
        except_cols.append("dummy")
        # De-duplicating except cols list
        except_cols = list(set(except_cols))
        except_cols_const = ",".join(except_cols)
        transformed_cols_final = (
            transformed_cols_const + "," + temp_null_valued_cols_const
        )
        if transformed_cols_final == ",":
            transformed_cols_final = ""

        level_cols_const = ",".join(level_cols)

        ############ Validation table creation and deleted rows persistance #############
        wc = WarehouseConnector(warehouse, models.Variable.get("warehouse_kwargs"))
        validated_table_name = wc._get_complete_table_name(f"{self.entity_type}_validated_table", False)
        raw_table_name = wc._get_complete_table_name(self.raw_table, True)
        anomaly_table_name = wc._get_complete_table_name(f"{self.entity_type}_anomaly", False)
        anomaly_summary_table_name = wc._get_complete_table_name(f"{self.entity_type}_anomaly_summary", False)
        null_check_const = "' , '".join(null_check)

        query = build_transform_validate_query(validated_table_name, raw_table_name, anomaly_table_name, anomaly_summary_table_name, except_cols_const,
                                                  transformed_cols_final, models.Variable.get(f'where_filter_{self.entity_type}'), old_cols, typecast_cols_const,
                                                      null_cols_const, level_cols_const, self.entity_type, null_check_const)

        models.Variable.set(
            f"{self.entity_type}_transformation_validation_query", query
        )

    def create_anomaly_metrics(self):
        import pandas as pd
        from airflow import models
        from database_utility.GenericDatabaseConnector import WarehouseConnector
        from queries.query_builder import build_anomaly_metric_summary_query

        mapping_data = pd.DataFrame.from_dict(
            eval(
                models.Variable.get(
                    f"mapping_data_{self.entity_type}", default_var=0
                ).replace(": nan", ": None")
            )
        )
        null_check =map(lambda x: x.lower(),mapping_data[
            (
                (mapping_data.source_column_name.notnull())
                & (mapping_data["is_null_allowed"] == False)
            )
        ].source_column_name.to_list())
        
        level_cols = mapping_data[
            (mapping_data.source_column_name.notnull()) & (mapping_data["unique_by"] == True)
        ].source_column_name.to_list()

        wc = WarehouseConnector(models.Variable.get("warehouse"), models.Variable.get("warehouse_kwargs"))
        validated_table_name = wc._get_complete_table_name(f"{self.entity_type}_validated_table", False)
        raw_table_name = wc._get_complete_table_name(self.raw_table, True)
        null_cols_const= f"""{"','".join(null_check)}"""
        level_cols_const = f"""{" ' ".join(level_cols)}"""

        anomaly_metric_table_query = build_anomaly_metric_summary_query(validated_table_name, raw_table_name, null_cols_const, 
                                                                            level_cols_const, models.Variable.get(f'where_filter_{self.entity_type}')) 
        models.Variable.set(
            f"{self.entity_type}_anomaly_metric_query", anomaly_metric_table_query
        )

    @abstractmethod
    def create_tables(self):
        pass
