# Get kpi_master and rule engine table

def get_tables(mod_name: str):
    from airflow.models import Variable

    if eval(Variable.get("read_from_db_flag", default_var="False")):
        return get_tables_from_db(mod_name)
    else:
        return get_tables_from_warehouse(mod_name)


# Get kpi_master and rule engine table from database
def get_tables_from_db(mod_name):

    import urllib
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    if mod_name == "derived_table":
        rule_query_string = """
            select * from data_platform.rule_master where action <> 'REPORT' and not is_deleted
        """
    elif mod_name == "post_derived_table":
        rule_query_string = """
            select * from data_platform.rule_master where action = 'REPORT' and not is_deleted
        """
    else:
        rule_query_string = """
            select * from data_platform.rule_master where not is_deleted
        """

    kpi_query_string = """
        select * from data_platform.kpi_master where not is_deleted
    """

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

    kpi_master = pd.read_sql(kpi_query_string, db_engine)

    rule_engine = pd.read_sql(rule_query_string, db_engine)

    return kpi_master, rule_engine


# Get kpi_master and rule engine table from database
def get_tables_from_warehouse(mod_name):

    import pandas as pd
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    rule_master_table_name = wc._get_complete_table_name(
        Variable.get("rule_master_name"), False
    )

    if mod_name == "derived_table":
        rule_query_string = f"""
        SELECT *
        FROM {rule_master_table_name}
        where action<>"REPORT"
        """
    elif mod_name == "post_derived_table":
        rule_query_string = f"""
        SELECT *
        FROM {rule_master_table_name}
        where action="REPORT"
        """
    else:
        rule_query_string = f"""
        SELECT *
        FROM {rule_master_table_name}
        """

    kpi_master_table_name = wc._get_complete_table_name(
        Variable.get("kpi_master_name"), False
    )
    kpi_query_string = f"""
    SELECT *
    from {kpi_master_table_name} 
    """

    kpi_master = wc.execute_query(kpi_query_string, True)
    rule_engine = wc.execute_query(rule_query_string, True)

    return kpi_master, rule_engine


def cross_validation(source_list, source_tab, destination_list, destination_tab):
    """
    ARGS: source_list - list consisting of source table level
    RETURN: String with joining the level of data for different tables
    """
    source_lower = [item.lower() for item in source_list]
    destination_lower = [item.lower() for item in destination_list]
    cols = list(set(source_lower).intersection(set(destination_lower)))
    new_cols = []
    for i in cols:
        new_cols.append(f"{source_tab}.{i} = {destination_tab}.{i}")
    where_condition = " and ".join([f"({str(i)})" for i in new_cols])
    return where_condition


def kpi_eval(kpi_list, mod_name):
    """this function evaluates the kpis and forms a dictionary by kpi as keys and kpi query as the values
    args: kpi_list [list of kpis]
    return : dict with kpi with its queries"""
    kpi_master, rule_engine = get_tables(mod_name)
    kpi_dict = {}
    tab_list = []
    if len(kpi_list) > 1:
        for i in kpi_list:
            tab_list += list(
                eval(
                    kpi_master[kpi_master["kpicode"] == i].reset_index(drop=True)[
                        "table"
                    ][0]
                ).keys()
            )
        if len(set(tab_list)) > 1:
            for i in range(len(kpi_list)):
                temp = kpi_master[kpi_master["kpicode"] == kpi_list[i]].reset_index(
                    drop=True
                )
                val = temp["query"][0]
                if "KPI" in val:
                    print("calculate another_kpi")
                else:
                    kpi_dict[kpi_list[i]] = {tab_list[i]: val}
        elif len(set(tab_list)) == 1:
            for i in kpi_list:
                temp = kpi_master[kpi_master["kpicode"] == i].reset_index(drop=True)
                val = temp["query"][0]
                if "KPI" in val:
                    print("calculate another_kpi")
                else:
                    kpi_dict[i] = val
    else:
        for i in kpi_list:
            temp = kpi_master[kpi_master["kpicode"] == i].reset_index(drop=True)
            val = temp["query"][0]
            if "KPI" in val:
                print("calculate another_kpi")
            else:
                kpi_dict[i] = val
    print("kpi_dict", kpi_dict)
    return kpi_dict


def summary_report(
    table,
    where_condition,
    module,
    rule,
    rule_display_name,
    rule_description,
    action,
    source_tab,
    destination_tab,
    join_condition,
):
    from airflow.models import Variable
    from google.cloud.exceptions import NotFound
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import (
        generate_create_table_ddl,
        generate_insert_ddl,
        datetime_const,
    )
    from google.cloud.bigquery import Row

    """
    args : table[table name],where_condition,module,rule,action
    function creates a summary report for the qc's
    """
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    final_table = wc._get_complete_table_name(f"{table}_validated_table", False)
    anomaly_table = wc._get_complete_table_name(f"{table}_anomaly_summary", False)
    query = f"select count(*) from {final_table}"
    data = wc.execute_query(query, True)
    no_original_rows = data.iloc[0, 0]
    if where_condition == "EMPTY":
        total_affected_rows = 0
    else:
        if module == "table_specific":
            query = f"select count(*) from {final_table} where {where_condition}"
        else:
            source = wc._get_complete_table_name(source_tab, False)
            destination = wc._get_complete_table_name(destination_tab, False)
            query = f"""select count(*)
            from {source} as {source_tab}
            left join 
            {destination}  as {destination_tab}
            on {join_condition}
            where {where_condition}"""
        data = wc.execute_query(query, True)
        print(query)
        total_affected_rows = data.iloc[0, 0]
    if no_original_rows == 0:
        loss_percent = 0
    else:
        loss_percent = (total_affected_rows / no_original_rows) * 100
    if total_affected_rows == 0:
        status = "Pass"
    else:
        status = "Fail"
    print(
        no_original_rows,
        total_affected_rows,
        loss_percent,
    )
    # table_id = f"{Variable.get('gcp_project')}.{Variable.get('dataset')}.{anomaly_table}"
    table_id = anomaly_table
    try:
        wc.table_exists(f"{table}_anomaly_summary", True)
    except NotFound:
        print("Table {} is not found.".format(table_id))
        table_schema = {
            "table": "string",
            "module": "string",
            "rule": "string",
            "rule_display_name": "string",
            "rule_description": "string",
            "action": "string",
            "no_original_rows": "int",
            "affected_rows": "int",
            "affected_rows_percent": "float",
            "status": "string",
            "insertion_date": "datetime",
        }
        query = generate_create_table_ddl(warehouse, anomaly_table, table_schema)
        wc.execute_query(query, True)
    data_dict = {
        "table": table,
        "module": module,
        "rule": rule,
        "rule_display_name": rule_display_name,
        "rule_description": rule_description,
        "action": action,
        "no_original_rows": no_original_rows,
        "affected_rows": total_affected_rows,
        "affected_rows_percent": loss_percent,
        "status": status,
        "insertion_date": datetime_const(warehouse),
    }
    query = generate_insert_ddl(warehouse, anomaly_table, data_dict)
    wc.execute_query(query, True)


def action_agg(
    table,
    stage,
    kpi_list,
    kpi_dict,
    inner_filter,
    outer_filter,
    thresold,
    rule_name,
    rule,
    rule_display_name,
    rule_description,
    group_by,
    mod_name,
):
    # agg_func = ["APPROX_COUNT_DISTINCT","AVG","CHECKSUM_AGG","COUNT","COUNT_BIG","GROUPING","GROUPING_ID","MAX","MIN","STDEV","STDEVP","STRING_AGG","SUM","VAR","VARP"]
    from airflow.models import Variable
    from google.cloud.exceptions import NotFound
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import generate_create_table_ddl, generate_insert_ddl, datetime_const
    
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    final_table = wc._get_complete_table_name(f"{table}_validated_table", False)
    anomaly_table = wc._get_complete_table_name(f"{table}_anomaly_summary", False)
    kpi_master, rule_master = get_tables(mod_name)
    group_by_list = []
    group_by_str = ""
    if group_by:
        group_by_list = eval(
            rule_master[rule_master["rule"] == rule]["group_by"].reset_index(drop=True)[
                0
            ]
        )
        group_by_str = ",".join([f"{str(i)}" for i in group_by_list])
    where_condition = outer_filter
    inner_where_condition = inner_filter

    if inner_where_condition:
        for i in kpi_list:  
            inner_where_condition = inner_where_condition.replace(i, (kpi_dict[i])) 
        # inner_where_condition = [
        #     inner_where_condition.replace(i, (kpi_dict[i])) for i in kpi_list
        # ]
    select_field_list = []
    if group_by_str:
        select_field_list.append(group_by_str)
    for i in kpi_list:
        if i in where_condition:
            temp = f"{kpi_dict[i]} {i}"
            select_field_list.append(temp)
    select_field = ",".join([f"{str(i)}" for i in select_field_list])
    base_query = f"SELECT {select_field} FROM {final_table}"
    where_clause = f"WHERE {inner_where_condition}" if inner_filter else ""
    group_by_clause = f"GROUP BY {group_by_str}" if group_by_list else ""

    if where_condition:
        for i in kpi_list:  
            where_condition_outer_filter = where_condition.replace(i, (kpi_dict[i])) 
    
    # Use HAVING for filters on aggregated columns, otherwise use WHERE.
    if group_by_list:
        # The where_condition contains KPIs which are aggregated, so it belongs in a HAVING clause.
        # We need to replace the KPI alias with the actual aggregation formula for the HAVING clause.
        having_condition = where_condition
        for kpi, formula in kpi_dict.items():
            having_condition = having_condition.replace(kpi, formula)
        filter_clause = f"HAVING {having_condition}"
    else:
        # No aggregation, so the filter belongs in a WHERE clause.
        filter_clause = f"WHERE {where_condition_outer_filter}" if where_condition_outer_filter else ""

    # Combine clauses into the final query.
    # Note: If there's an inner_filter (WHERE) and an outer_filter (HAVING), they will be applied correctly.
    # If there's no aggregation, the filter_clause will be a WHERE, and we must handle the case where
    # an inner_filter (another WHERE) already exists.
    if not group_by_list and inner_filter and where_condition:
        # Both filters apply to non-aggregated data, so they should be combined with AND.
        where_clause = f"WHERE {inner_where_condition} AND {where_condition_outer_filter}"
        filter_clause = ""

    query = f""" {base_query}
    {where_clause}
    {group_by_clause}
    {filter_clause}
    LIMIT 100
    """
    rows = wc.execute_query(query, True)
    print(query)
    if rows.shape[0] == 0:
        print("qc passed")
        status = "Pass"
    else:
        status = "Fail"
    try:
        wc.table_exists(f"{table}_anomaly_summary", True)

    except NotFound:
        table_schema = {
            "table": "string",
            "module": "string",
            "rule": "string",
            "rule_display_name": "string",
            "rule_description": "string",
            "action": "string",
            "no_original_rows": "int",
            "affected_rows": "int",
            "affected_rows_percent": "float",
            "status": "string",
            "insertion_date": "datetime",
        }

        query = generate_create_table_ddl(warehouse, anomaly_table, table_schema)
        rows = wc.execute_query(query, True)

    data_dict = {
        "table": table,
        "module": stage,
        "rule": rule_name,
        "rule_display_name": rule_display_name,
        "rule_description": rule_description,
        "action": "REPORT_AGG",
        "no_original_rows": 0,
        "affected_rows": 0,
        "affected_rows_percent": 0.0,
        "status": status,
        "insertion_date": datetime_const(warehouse),
    }
    query = generate_insert_ddl(warehouse, anomaly_table, data_dict)
    rows = wc.execute_query(query, True)
    print(query)


def take_action(
    stage,
    rule_name,
    rule_display_name,
    rule_description,
    where_condition,
    rule_action,
    value_tobereplaced,
    rule_on_table,
    kpi_list,
    source_list,
    destination_list,
    join_condition,
    pulltype_flag,
    mod_name,
):

    import pandas as pd
    from airflow.models import Variable
    from google.cloud.exceptions import NotFound
    import re
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import datetime_const

    """
    args : stage [stage at which its triggered],rule_name [rule name which is to be executed],inner_filter,where_condtion[where condition used]
    """

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    # final_table = wc._get_complete_table_name(f"{table}_validated_table", False)
    anomaly_table = wc._get_complete_table_name(f"{rule_on_table}_anomaly", False)
    kpi_master, rule_engine = get_tables(mod_name)
    source_tab = ""
    destination_tab = ""

    if rule_action == "DELETE":
        if stage == "table_specific":
            table_name = wc._get_complete_table_name(
                f"{rule_on_table}_validated_table", False
            )
            tab = rule_on_table + "_validated_table"
            table_column_names = wc._get_table_columns(tab)
            table_column_names = ",".join([f"{str(i)}" for i in table_column_names])
            final_column_names = table_column_names + ",QC,action,insertion_date"
            summary_report(
                rule_on_table,
                where_condition,
                stage,
                rule_name,
                rule_display_name,
                rule_description,
                rule_action,
                source_tab,
                destination_tab,
                join_condition,
            )
            try:
                wc.table_exists(f"{rule_on_table}_anomaly", True)
                query = f"""insert into {anomaly_table} ({final_column_names})
                select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                from {table_name} 
                where {where_condition}"""
                Variable.set(f"{rule_on_table}_query", query)
                print("Query to insert anomaly into the table\n", query)
                rows = wc.execute_query(query, False)
                action_query = f"delete from {table_name} as {rule_on_table}_validated_table where {where_condition}"
                rows = wc.execute_query(action_query, False)
                if rows == 0:
                    print("qc failed")

            except NotFound:
                query = f"create table {anomaly_table} as select *,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date from {table_name} where {where_condition}"
                Variable.set(f"{rule_on_table}_queryy", query)
                print("Query to create anomaly into the table\n", query)
                rows = wc.execute_query(query, False)
                action_query = f"delete from {table_name} as {rule_on_table}_validated_table where {where_condition}"
                rows = wc.execute_query(action_query, False)
                print(action_query)
                if rows == 0:
                    print("qc failed")

        elif stage == "cross_validation":
            anomaly_table = wc._get_complete_table_name(
                rule_on_table + "_anomaly", False
            )
            table_name = wc._get_complete_table_name(
                rule_on_table + "_validated_table", False
            )
            temp_tab1 = list(
                eval(
                    kpi_master[kpi_master["kpicode"] == kpi_list[0]].reset_index(
                        drop=True
                    )["table"][0]
                ).keys()
            )[0]
            temp_tab2 = list(
                eval(
                    kpi_master[kpi_master["kpicode"] == kpi_list[1]].reset_index(
                        drop=True
                    )["table"][0]
                ).keys()
            )[0]
            pulltype_flag = 0
            if rule_on_table == temp_tab1:
                source_tab = temp_tab1 + "_validated_table"
                destination_tab = temp_tab2 + "_validated_table"
                table_column_names_list = wc._get_table_columns(source_tab)
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                final_column_names = table_column_names + f",QC,action,insertion_date"
                table_column_names_list = [
                    f"{source_tab}." + i for i in table_column_names_list
                ]
                table_column_names = ""
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                summary_report(
                    rule_on_table,
                    where_condition,
                    stage,
                    rule_name,
                    rule_display_name,
                    rule_description,
                    rule_action,
                    source_tab,
                    destination_tab,
                    join_condition,
                )
                try:
                    source_tab_complete_name = wc._get_complete_table_name(
                        temp_tab1 + "_validated_table", False
                    )
                    destination_tab_complete_name = wc._get_complete_table_name(
                        temp_tab2 + "_validated_table", False
                    )
                    wc.table_exists(f"{rule_on_table}_anomaly", True)
                    query = f"""insert into {anomaly_table} ({final_column_names})
                    select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                    from {source_tab_complete_name} as {temp_tab1}_validated_table
                    left join
                    {destination_tab_complete_name} as {temp_tab2}_validated_table
                    on {join_condition} 
                    where {where_condition}"""
                    print("Query to insert anomaly into the table\n", query)
                    rows = wc.execute_query(query, False)
                    source_lower = [item.lower() for item in source_list]
                    destination_lower = [item.lower() for item in destination_list]
                    cols = list(set(source_lower).intersection(set(destination_lower)))
                    new_cols = []
                    for i in cols:
                        new_cols.append(f"{source_tab}.{i}")
                    select_level = ",".join([f"({str(i)})" for i in new_cols])
                    action_query = f"""delete from {source_tab_complete_name}  as {temp_tab1}_validated_table
                    where concat({select_level}) in 
                    (select concat({select_level})
                    from {source_tab_complete_name} as {temp_tab1}_validated_table
                    left join
                    {destination_tab_complete_name} as {temp_tab2}_validated_table
                    on {join_condition}
                    where {where_condition})"""
                    print("Deletion query", action_query)
                    rows = wc.execute_query(action_query, False)
                    print(action_query)
                    if rows == -1:
                        print("qc failed")

                except NotFound:
                    query = f"""create table {anomaly_table} as
                    select {source_tab}.*,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                    from {source_tab_complete_name} as {temp_tab1}_validated_table
                    left join
                    {destination_tab_complete_name} as {temp_tab2}_validated_table
                    on {join_condition} 
                    where {where_condition}"""
                    rows = wc.execute_query(query, False)
                    print("Query to create anomaly into the table\n", query)
                    source_lower = [item.lower() for item in source_list]
                    destination_lower = [item.lower() for item in destination_list]
                    cols = list(set(source_lower).intersection(set(destination_lower)))
                    new_cols = []
                    for i in cols:
                        new_cols.append(f"{source_tab}.{i}")
                    select_level = ",".join([f"({str(i)})" for i in new_cols])
                    action_query = f"""delete from {source_tab_complete_name} as {temp_tab1}_validated_table
                    where concat({select_level}) in 
                    (select concat({select_level})
                    from {source_tab_complete_name} as {temp_tab1}_validated_table"
                    left join
                    {destination_tab_complete_name} as {temp_tab2}_validated_table"
                    on {join_condition}
                    where {where_condition})"""
                    rows = wc.execute_query(action_query)
                    print(action_query)
                    if rows == 0:
                        print("qc failed")

            elif rule_on_table == temp_tab2:
                source_tab = temp_tab2 + "_validated_table"
                destination_tab = temp_tab1 + "_validated_table"
                source_tab_complete_name = wc._get_complete_table_name(
                    source_tab, False
                )
                destination_tab_complete_name = wc._get_complete_table_name(
                    destination_tab, False
                )
                table_column_names_list = wc._get_table_columns(source_tab)
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                final_column_names = table_column_names + f",QC,action,insertion_date"
                table_column_names_list = [
                    f"{source_tab}." + i for i in table_column_names_list
                ]
                table_column_names = ""
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                summary_report(
                    rule_on_table,
                    where_condition,
                    stage,
                    rule_name,
                    rule_display_name,
                    rule_description,
                    rule_action,
                    source_tab,
                    destination_tab,
                    join_condition,
                )
                try:
                    wc.table_exists(f"{rule_on_table}_anomaly", True)
                    query = f"""insert into {anomaly_table} ({final_column_names})
                    select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition} 
                    where {where_condition}"""
                    print(query)
                    rows = wc.execute_query(query, False)
                    source_lower = [item.lower() for item in source_list]
                    destination_lower = [item.lower() for item in destination_list]
                    cols = list(set(source_lower).intersection(set(destination_lower)))
                    new_cols = []
                    for i in cols:
                        new_cols.append(f"{source_tab}.{i}")
                    select_level = ",".join([f"({str(i)})" for i in new_cols])
                    action_query = f"""delete from {source_tab_complete_name} as {source_tab}
                    where concat({select_level}) in 
                    (select concat({select_level})
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition}
                    where {where_condition})"""
                    rows = wc.execute_query(action_query, False)
                    print(action_query)
                    if rows == 0:
                        print("qc failed")

                except NotFound:
                    query = f"""create table {anomaly_table} as
                    select {source_tab}.*,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition} 
                    where {where_condition}"""
                    rows = wc.execute_query(query, False)
                    print(query)
                    source_lower = [item.lower() for item in source_list]
                    destination_lower = [item.lower() for item in destination_list]
                    cols = list(set(source_lower).intersection(set(destination_lower)))
                    new_cols = []
                    for i in cols:
                        new_cols.append(f"{source_tab}.{i}")
                    select_level = ",".join([f"({str(i)})" for i in new_cols])
                    action_query = f"""delete from {source_tab_complete_name} as {source_tab}
                    where concat({select_level}) in 
                    (select concat({select_level})
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition}
                    where {where_condition})"""
                    rows = wc.execute_query(action_query, False)
                    if rows == 0:
                        print("qc failed")

    elif rule_action == "REPLACE":
        if stage == "table_specific":
            table_name = wc._get_complete_table_name(
                rule_on_table + "_validated_table", False
            )
            tab = rule_on_table + "_validated_table"
            table_column_names = wc._get_table_columns(tab)
            table_column_names = ",".join([f"{str(i)}" for i in table_column_names])
            final_column_names = table_column_names + ",QC,action,insertion_date"
            summary_report(
                rule_on_table,
                where_condition,
                stage,
                rule_name,
                rule_display_name,
                rule_description,
                rule_action,
                source_tab,
                destination_tab,
                join_condition,
            )
            try:
                wc.table_exists(f"{rule_on_table}_anomaly", True)
                query = f"""insert into {anomaly_table} ({final_column_names})
                select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                from {table_name} 
                where {where_condition}"""
                rows = wc.execute_query(query, False)
                regex = re.compile(r"KPI_[0-9]+")
                val_list = regex.findall(str(value_tobereplaced))
                val_list = list(set(val_list))
                val_dict = kpi_eval(val_list, mod_name)
                value = value_tobereplaced
                for i in val_list:
                    value = value.replace(i, (val_dict[i]))
                print(value)
                action_query = (
                    f"update {table_name} set {value} where {where_condition}"
                )
                rows = wc.execute_query(action_query, False)
                if rows == 0:
                    print("qc failed")

            except NotFound:
                query = f"create table {anomaly_table} as select *,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date from {table_name} where {where_condition}"
                print(query)
                rows = wc.execute_query(query, False)
                regex = re.compile(r"KPI_[0-9]+")
                val_list = regex.findall(str(value_tobereplaced))
                val_list = list(set(val_list))
                val_dict = kpi_eval(val_list, mod_name)
                value = value_tobereplaced
                for i in val_list:
                    value = value.replace(i, (val_dict[i]))
                print(value)
                action_query = (
                    f"update {table_name} set {value} where {where_condition}"
                )
                rows = wc.execute_query(action_query)
                if rows == 0:
                    print("qc failed")

        elif stage == "cross_validation":
            anomaly_table = wc._get_complete_table_name(
                f"{rule_on_table}_anomaly", False
            )
            temp_tab1 = list(
                eval(
                    kpi_master[kpi_master["kpicode"] == kpi_list[0]].reset_index(
                        drop=True
                    )["table"][0]
                ).keys()
            )[0]
            temp_tab2 = list(
                eval(
                    kpi_master[kpi_master["kpicode"] == kpi_list[1]].reset_index(
                        drop=True
                    )["table"][0]
                ).keys()
            )[0]
            table_name = wc._get_complete_table_name(
                f"{rule_on_table}_validated_table", False
            )
            pulltype_flag = 0
            if rule_on_table == temp_tab1:
                summary_report(
                    rule_on_table,
                    where_condition,
                    stage,
                    rule_name,
                    rule_display_name,
                    rule_description,
                    rule_action,
                    source_tab,
                    destination_tab,
                    join_condition,
                )
                source_tab = temp_tab1 + "_validated_table"
                destination_tab = temp_tab2 + "_validated_table"
                source_tab_complete_name = wc._get_complete_table_name(
                    source_tab, False
                )
                destination_tab_complete_name = wc._get_complete_table_name(
                    destination_tab, False
                )
                table_column_names_list = wc._get_table_columns(source_tab)
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                final_column_names = table_column_names + f",QC,action,insertion_date"
                table_column_names = ""
                table_column_names_list = [
                    f"{source_tab}." + i for i in table_column_names_list
                ]
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                try:
                    wc.table_exists(f"{rule_on_table}_anomaly", True)
                    regex = re.compile(r"KPI_[0-9]+")
                    val_list = regex.findall(str(value_tobereplaced))
                    val_list = list(set(val_list))
                    val_dict = kpi_eval(val_list, mod_name)
                    value = value_tobereplaced
                    for i in val_list:
                        value = value.replace(i, (val_dict[i]))
                    print(value)
                    query = f"""insert into {anomaly_table} ({final_column_names})
                    select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition}  
                    where {where_condition}"""
                    print(query)
                    rows = wc.execute_query(query, False)
                    source_lower = [item.lower() for item in source_list]
                    destination_lower = [item.lower() for item in destination_list]
                    cols = list(set(source_lower).intersection(set(destination_lower)))
                    new_cols = []
                    for i in cols:
                        new_cols.append(f"{source_tab}.{i}")
                    select_level = ",".join([f"({str(i)})" for i in new_cols])
                    action_query = f"""update {source_tab_complete_name} 
                    set {value}
                    where concat({select_level}) in 
                    (select concat({select_level})
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition}
                    where {where_condition})"""
                    print(query)
                    rows = wc.execute_query(action_query, False)
                    if rows == 0:
                        print("qc failed")

                except NotFound:
                    query = f"""create table {anomaly_table} as
                        select {source_tab}.*,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                        from {source_tab_complete_name} as {source_tab}
                        left join
                        {destination_tab_complete_name} as {destination_tab}
                        on {join_condition}
                        where {where_condition}"""
                    rows = wc.execute_query(query, False)
                    print(query)
                    source_lower = [item.lower() for item in source_list]
                    destination_lower = [item.lower() for item in destination_list]
                    cols = list(set(source_lower).intersection(set(destination_lower)))
                    new_cols = []
                    for i in cols:
                        new_cols.append(f"{source_tab}.{i}")
                    select_level = ",".join([f"({str(i)})" for i in new_cols])
                    action_query = f"""update {source_tab_complete_name} 
                        set {value}
                        where concat({select_level})in 
                        (select concat({select_level})
                        from {source_tab_complete_name} as {source_tab}
                        left join
                        {destination_tab_complete_name} as {destination_tab}
                        on {join_condition}
                        where {where_condition})"""
                    print(query)
                    rows = wc.execute_query(action_query)
                    if rows == 0:
                        print("qc failed")

            elif rule_on_table == temp_tab2:
                summary_report(
                    rule_on_table,
                    where_condition,
                    stage,
                    rule_name,
                    rule_display_name,
                    rule_description,
                    rule_action,
                    source_tab,
                    destination_tab,
                    join_condition,
                )
                source_tab = temp_tab2 + "_validated_table"
                destination_tab = temp_tab1 + "_validated_table"
                source_tab_complete_name = wc._get_complete_table_name(
                    source_tab, False
                )
                destination_tab_complete_name = wc._get_complete_table_name(
                    destination_tab, False
                )
                table_column_names_list = wc._get_table_columns(source_tab)
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                final_column_names = table_column_names + f",QC,action,insertion_date"
                table_column_names = ""
                table_column_names_list = [
                    f"{source_tab}." + i for i in table_column_names_list
                ]
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                try:
                    wc.table_exists(f"{rule_on_table}_anomaly", True)
                    regex = re.compile(r"KPI_[0-9]+")
                    val_list = regex.findall(str(value_tobereplaced))
                    val_list = list(set(val_list))
                    val_dict = kpi_eval(val_list, mod_name)
                    value = value_tobereplaced
                    for i in val_list:
                        value = value.replace(i, (val_dict[i]))
                    print(value)
                    query = f"""insert into {anomaly_table} ({final_column_names})
                    select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition}
                    where {where_condition}"""
                    print(query)
                    rows = wc.execute_query(query, False)
                    source_lower = [item.lower() for item in source_list]
                    destination_lower = [item.lower() for item in destination_list]
                    cols = list(set(source_lower).intersection(set(destination_lower)))
                    new_cols = []
                    for i in cols:
                        new_cols.append(f"{source_tab}.{i}")
                    select_level = ",".join([f"({str(i)})" for i in new_cols])
                    action_query = f"""update {source_tab_complete_name} 
                    set {value}
                    where concat({select_level})in 
                    (select concat({select_level})
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition}
                    where {where_condition})"""
                    print(query)
                    rows = wc.execute_query(action_query)
                    if rows == 0:
                        print("qc failed")

                except NotFound:
                    query = f"""create table {anomaly_table} as
                        select {source_tab}.*,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                        from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                        on {join_condition}  
                        where {where_condition}"""
                    rows = wc.execute_query(query, False)
                    print(query)
                    source_lower = [item.lower() for item in source_list]
                    destination_lower = [item.lower() for item in destination_list]
                    cols = list(set(source_lower).intersection(set(destination_lower)))
                    new_cols = []
                    for i in cols:
                        new_cols.append(f"{source_tab}.{i}")
                    select_level = ",".join([f"({str(i)})" for i in new_cols])
                    action_query = f"""update {source_tab_complete_name} 
                        set {value}
                        where concat({select_level})in 
                        (select concat({select_level})
                        from {source_tab_complete_name} as {source_tab}
                        left join
                        {destination_tab_complete_name} as {destination_tab}
                        on {join_condition}
                        where {where_condition})"""
                    print(query)
                    rows = wc.execute_query(action_query)
                    if rows == 0:
                        print("qc failed")

    elif rule_action == "REPORT":
        if stage == "table_specific":
            table_name = wc._get_complete_table_name(
                f"{rule_on_table}_validated_table", False
            )
            tab = rule_on_table + "_validated_table"
            table_column_names = wc._get_table_columns(tab)
            table_column_names = ",".join([f"{str(i)}" for i in table_column_names])
            final_column_names = table_column_names + ",QC,action,insertion_date"
            summary_report(
                rule_on_table,
                where_condition,
                stage,
                rule_name,
                rule_display_name,
                rule_description,
                rule_action,
                source_tab,
                destination_tab,
                join_condition,
            )
            try:
                wc.table_exists(f"{rule_on_table}_anomaly", True)
                query = f"""insert into {anomaly_table} ({final_column_names})
                select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                from {table_name} 
                where {where_condition}"""
                print(query)
                rows = wc.execute_query(query, False)
                if rows == 0:
                    print("qc failed")

            except NotFound:
                query = f"create table {anomaly_table} as select *,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date from {table_name} where {where_condition}"
                print(query)
                rows = wc.execute_query(query, False)
                if rows == 0:
                    print("qc failed")

        elif stage == "cross_validation":
            anomaly_table = wc._get_complete_table_name(
                f"{rule_on_table}_anomaly", False
            )
            temp_tab1 = list(
                eval(
                    kpi_master[kpi_master["kpicode"] == kpi_list[0]].reset_index(
                        drop=True
                    )["table"][0]
                ).keys()
            )[0]
            temp_tab2 = list(
                eval(
                    kpi_master[kpi_master["kpicode"] == kpi_list[1]].reset_index(
                        drop=True
                    )["table"][0]
                ).keys()
            )[0]
            table_name = wc._get_complete_table_name(
                f"{rule_on_table}_validated_table", False
            )
            pulltype_flag = 0
            print(rule_on_table, temp_tab1)
            print(rule_on_table, temp_tab2)
            if rule_on_table == temp_tab1:
                source_tab = temp_tab1 + "_validated_table"
                destination_tab = temp_tab2 + "_validated_table"
                source_tab_complete_name = wc._get_complete_table_name(
                    source_tab, False
                )
                destination_tab_complete_name = wc._get_complete_table_name(
                    destination_tab, False
                )
                summary_report(
                    rule_on_table,
                    where_condition,
                    stage,
                    rule_name,
                    rule_display_name,
                    rule_description,
                    rule_action,
                    source_tab,
                    destination_tab,
                    join_condition,
                )
                table_column_names_list = wc._get_table_columns(source_tab)
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                final_column_names = table_column_names + f",QC,action,insertion_date"
                table_column_names = ""
                table_column_names_list = [
                    f"{source_tab}." + i for i in table_column_names_list
                ]
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                try:
                    wc.table_exists(f"{rule_on_table}_anomaly", True)
                    query = f"""insert into {anomaly_table} ({final_column_names})
                    select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}
                    on {join_condition}  
                    where {where_condition}"""
                    print("\n", query)
                    rows = wc.execute_query(query, False)
                    if rows == 0:
                        print("qc failed")

                except NotFound:
                    query = f"""create table {anomaly_table} as select {source_tab}.*,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                                from {source_tab_complete_name} as {source_tab}
                                left join
                                {destination_tab_complete_name} as {destination_tab}
                                on {join_condition} where {where_condition}"""
                    rows = wc.execute_query(query, False)
                    print("\n", query)
                    if rows == 0:
                        print("qc failed")

            elif rule_on_table == temp_tab2:
                source_tab = temp_tab2 + "_validated_table"
                destination_tab = temp_tab1 + "_validated_table"
                source_tab_complete_name = wc._get_complete_table_name(
                    source_tab, False
                )
                destination_tab_complete_name = wc._get_complete_table_name(
                    destination_tab, False
                )
                summary_report(
                    rule_on_table,
                    where_condition,
                    stage,
                    rule_name,
                    rule_display_name,
                    rule_description,
                    rule_action,
                    source_tab,
                    destination_tab,
                    join_condition,
                )

                table_column_names_list = wc._get_table_columns(source_tab)
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                final_column_names = table_column_names + f",QC,action,insertion_date"
                table_column_names = ""
                table_column_names_list = [
                    f"{source_tab}." + i for i in table_column_names_list
                ]
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                try:
                    wc.table_exists(anomaly_table, True)
                    query = f"""insert into {anomaly_table} ({final_column_names})
                    select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}  
                    on {join_condition}  
                    where {where_condition}"""
                    print(query)
                    rows = wc.execute_query(query, False)
                    if rows == 0:
                        print("qc failed")

                except NotFound:
                    query = f"""create table {anomaly_table} as 
                        select {source_tab}.*,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                        from {source_tab_complete_name} as {source_tab}
                        left join
                        {destination_tab_complete_name} as {destination_tab}
                        on {join_condition} where {where_condition}"""
                    rows = wc.execute_query(query, False)
                    print(query)
                    if rows == -1:
                        print("qc failed")

    elif rule_action == "PAUSE":
        if stage == "table_specific":
            table_name = table_name = wc._get_complete_table_name(
                f"{rule_on_table}_validated_table", False
            )
            tab = rule_on_table + "_validated_table"
            table_column_names = wc._get_table_columns(tab)
            table_column_names = ",".join([f"{str(i)}" for i in table_column_names])
            final_column_names = table_column_names + ",QC,action,insertion_date"
            summary_report(
                rule_on_table,
                where_condition,
                stage,
                rule_name,
                rule_display_name,
                rule_description,
                rule_action,
                source_tab,
                destination_tab,
                join_condition,
            )
            try:
                wc.table_exists(f"{rule_on_table}_anomaly", True)
                query = f"""insert into {anomaly_table} ({final_column_names})
                select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                from {table_name} 
                where {where_condition}"""
                rows = wc.execute_query(query, False)
                if rows == 0:
                    print("qc failed")

            except NotFound:
                query = f"create table {anomaly_table} as select *,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date from {table_name} where {where_condition}"
                print(query)
                rows = wc.execute_query(query, False)
                if rows == 0:
                    print("qc failed")

        elif stage == "cross_validation":
            anomaly_table = wc._get_complete_table_name(
                f"{rule_on_table}_anomaly", False
            )
            temp_tab1 = list(
                eval(
                    kpi_master[kpi_master["kpicode"] == kpi_list[0]].reset_index(
                        drop=True
                    )["table"][0]
                ).keys()
            )[0]
            temp_tab2 = list(
                eval(
                    kpi_master[kpi_master["kpicode"] == kpi_list[1]].reset_index(
                        drop=True
                    )["table"][0]
                ).keys()
            )[0]
            table_name = wc._get_complete_table_name(
                f"{rule_on_table}_validated_table", False
            )
            pulltype_flag = 0
            print(pulltype_flag)
            if rule_on_table == temp_tab1:
                source_tab = temp_tab1 + "_validated_table"
                destination_tab = temp_tab2 + "_validated_table"
                source_tab_complete_name = wc._get_complete_table_name(
                    source_tab, False
                )
                destination_tab_complete_name = wc._get_complete_table_name(
                    destination_tab, False
                )
                summary_report(
                    rule_on_table,
                    where_condition,
                    stage,
                    rule_name,
                    rule_display_name,
                    rule_description,
                    rule_action,
                    source_tab,
                    destination_tab,
                    join_condition,
                )

                table_column_names_list = wc._get_table_columns(source_tab)
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                final_column_names = table_column_names + f",QC,action,insertion_date"
                table_column_names = ""
                table_column_names_list = [
                    f"{source_tab}." + i for i in table_column_names_list
                ]
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                try:
                    wc.table_exists(f"{rule_on_table}_anomaly", True)
                    query = f"""insert into {anomaly_table} ({final_column_names})
                    select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date 
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}  
                    on {join_condition}  
                    where {where_condition}"""
                    rows = wc.execute_query(query, False)
                    if rows == 0:
                        print("qc failed")

                except NotFound:
                    query = f"""create table {anomaly_table} as select {source_tab}.*,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                                from {source_tab_complete_name} as {source_tab}
                                left join
                                {destination_tab_complete_name} as {destination_tab}
                                 on {join_condition} where {where_condition}"""
                    rows = wc.execute_query(query, False)
                    if rows == 0:
                        print("qc failed")

            elif rule_on_table == temp_tab2:
                source_tab = temp_tab2 + "_validated_table"
                destination_tab = temp_tab1 + "_validated_table"
                source_tab_complete_name = wc._get_complete_table_name(
                    source_tab, False
                )
                destination_tab_complete_name = wc._get_complete_table_name(
                    destination_tab, False
                )
                summary_report(
                    rule_on_table,
                    where_condition,
                    stage,
                    rule_name,
                    rule_display_name,
                    rule_description,
                    rule_action,
                    source_tab,
                    destination_tab,
                    join_condition,
                )
                table_column_names_list = wc._get_table_columns(source_tab)
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                final_column_names = table_column_names + f",QC,action,insertion_date"
                table_column_names = ""
                table_column_names_list = [
                    f"{source_tab}." + i for i in table_column_names_list
                ]
                table_column_names = ",".join(
                    [f"{str(i)}" for i in table_column_names_list]
                )
                try:
                    wc.table_exists(f"{rule_on_table}_anomaly", True)
                    query = f"""insert into {anomaly_table} ({final_column_names})
                    select {table_column_names},'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                    from {source_tab_complete_name} as {source_tab}
                    left join
                    {destination_tab_complete_name} as {destination_tab}  
                    on {join_condition}  
                    where {where_condition}"""
                    print(query)
                    rows = wc.execute_query(query, False)
                    if rows == 0:
                        print("qc failed")

                except NotFound:
                    query = f"""create table {anomaly_table} as 
                        select {source_tab}.*,'{rule_name}' as QC,'{rule_action}' as action,{datetime_const(warehouse)} as insertion_date
                        from {source_tab_complete_name} as {source_tab}
                        left join
                        {destination_tab_complete_name} as {destination_tab}
                        on {join_condition} where {where_condition}"""

                    rows = wc.execute_query(query, False)
                    print(query)
                    if rows == -1:
                        print("qc failed")


def get_rules(table, stage, mod_name):

    import re
    import pandas as pd

    kpi_master, rule_engine = get_tables(mod_name)
    regex = re.compile(table)
    rule_list = []
    rule_df = pd.DataFrame()
    for i in range(len(rule_engine)):
        if table == (list(eval(rule_engine["table"][i]).keys()))[0]:
            rule_list.append(rule_engine["rule"][i])
            rule_df = rule_engine[rule_engine["rule"].isin(rule_list)].reset_index(
                drop=True
            )
            rule_df = rule_df[rule_df["module"] == stage].reset_index(drop=True)
    print(rule_df.head(2))
    return rule_df


def check_thresold(stage, table, where_condition, thresold):
    import pandas as pd
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    flag = 0
    if stage == "table_specific":
        final_table = table + "_validated_table"
        final_table_complete_name = wc._get_complete_table_name(final_table, False)
        query_string = f"""
        SELECT 
            CASE 
                WHEN b.den = 0 THEN NULL 
                ELSE a.num / b.den 
            END AS f0_ 
        FROM 
        (SELECT COUNT(*) AS num FROM {final_table_complete_name} WHERE {where_condition}) a,
        (SELECT COUNT(*) AS den FROM {final_table_complete_name}) b;
        """
        val = wc.execute_query(query_string, True)
    if "%" in thresold:
        print(eval(str(val["f0_"][0]) + thresold.replace("%", "/100")))
        if eval(str(val["f0_"][0]) + thresold.replace("%", "/100")):
            flag = 1
    return flag


def check_table(table):
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    final_table = table + "_validated_table"
    final_table_complete_name = wc._get_complete_table_name(final_table, False)
    print("\n\n", final_table, "\n\n")

    query = f"select count(*) from {final_table_complete_name}"
    print(query)
    rows = wc.execute_query(query, True)
    no_original_rows = rows.iloc[0, 0]
    if no_original_rows == 0:
        return 0
    return 1


def query_formation(stage, table, mod_name):
    from airflow.models import Variable
    import re

    """This function groups the rules in according with the stage/module and returns the parameters required for the query formation
    args: stage[module at which this rules needs to be performed]
    return : params for the final queries"""

    kpi_master, rule_engine = get_tables(mod_name)
    rule_group = get_rules(table, stage, mod_name)
    print(table, "\n", stage)
    source_tab = ""
    destination_tab = ""
    source_list = []
    destination_list = []
    join_condition = ""
    where_condition = ""
    rule_action = ""
    pulltype_flag = 0
    if Variable.get("ingestion_type") != "periodic" and stage != "cross_validation":
        pulltype_flag = 1

    if Variable.get("ingestion_type") == "periodic" and stage == "table_specific":
        # to truncate the summary table before perodic ingestion
        # summmary_anomaly_table = (
        #     f"{Variable.get('gcp_project')}.{Variable.get('dataset')}."
        #     + table
        #     + "_anomaly_summary"
        # )
        pass

    if len(rule_group) > 0 and check_table(table):
        if stage == "table_specific":
            total_rules = len(rule_group)
            print(rule_group)
            for i in range(total_rules):
                rule_name = rule_group["name"][i]
                rule_display_name = rule_group["rule_display_name"][i]
                rule_description = rule_group["rule_description"][i]
                rule = rule_group["rule"][i]
                inner_filter = rule_group["inner_filter"][i]
                kpi_list = rule_group["kpis"][i]
                kpi_list = eval(kpi_list)
                thresold = rule_group["threshold"][i]
                rule_action = rule_group["action"][i]
                value_tobereplaced = rule_group["value"][i]
                outer_filter = rule_group["outer_filter"][i]
                group_by = rule_group["group_by"][i]
                # if rule_group['table'][i] == 'NULL':
                #     rule_on_table = 'NULL'
                # else:
                #     rule_on_table = list((eval(rule_group['table'][i])).keys())[0]
                agg = rule_group["agg"][i]
                kpi_dict = kpi_eval(kpi_list, mod_name)
                where_condition = inner_filter
                if str(agg).lower() != "false":
                    action_agg(
                        table,
                        stage,
                        kpi_list,
                        kpi_dict,
                        inner_filter,
                        outer_filter,
                        thresold,
                        rule_name,
                        rule,
                        rule_display_name,
                        rule_description,
                        group_by,
                        mod_name,
                    )
                else:
                    for j in kpi_list:
                        where_condition = where_condition.replace(j, (kpi_dict[j]))
                    Variable.set(f"thresold_qc_{rule_name}", thresold)
                    if thresold:
                        print(i, "\n\n")
                        flag = check_thresold(stage, table, where_condition, thresold)
                        print(thresold == None, "action", rule_action)
                        if flag:
                            take_action(
                                stage,
                                rule_name,
                                rule_display_name,
                                rule_description,
                                where_condition,
                                rule_action,
                                value_tobereplaced,
                                table,
                                kpi_list,
                                source_list,
                                destination_list,
                                join_condition,
                                pulltype_flag,
                                mod_name,
                            )
                            pulltype_flag = 0
                        else:
                            summary_report(
                                table,
                                where_condition,
                                stage,
                                rule_name,
                                rule_display_name,
                                rule_description,
                                rule_action,
                                source_tab,
                                destination_tab,
                                join_condition,
                            )
                    else:
                        take_action(
                            stage,
                            rule_name,
                            rule_display_name,
                            rule_description,
                            where_condition,
                            rule_action,
                            value_tobereplaced,
                            table,
                            kpi_list,
                            source_list,
                            destination_list,
                            join_condition,
                            pulltype_flag,
                            mod_name,
                        )
                        pulltype_flag = 0

        elif stage == "cross_validation":
            print("Inside cross validation", stage)
            total_rules = len(rule_group)
            for i in range(total_rules):
                rule_name = rule_group["name"][i]
                rule_display_name = rule_group["rule_display_name"][i]
                rule_description = rule_group["rule_description"][i]
                rule_code = rule_group["rule"][i]
                inner_filter = rule_group["inner_filter"][i]
                kpi_list = rule_group["kpis"][i]
                kpi_list = eval(kpi_list)
                thresold = rule_group["threshold"][i]
                rule_action = rule_group["action"][i]
                value_tobereplaced = rule_group["value"][i]
                # if rule_group['table'][i] == 'NULL':
                #     rule_on_table = 'NULL'
                # else:
                #     rule_on_table = list((eval(rule_group['table'][i])).keys())[0]
                agg = rule_group["agg"][i]
                temp_tab1 = list(
                    eval(
                        kpi_master[kpi_master["kpicode"] == kpi_list[0]].reset_index(
                            drop=True
                        )["table"][0]
                    ).keys()
                )[0]
                temp_tab2 = list(
                    eval(
                        kpi_master[kpi_master["kpicode"] == kpi_list[1]].reset_index(
                            drop=True
                        )["table"][0]
                    ).keys()
                )[0]
                print("\ntable:", temp_tab2, "\ntemp_tab1:", temp_tab1)
                if table == temp_tab1:
                    source_tab = temp_tab1 + "_validated_table"
                    destination_tab = temp_tab2 + "_validated_table"
                    source_list = list(
                        eval(
                            kpi_master[
                                kpi_master["kpicode"] == kpi_list[0]
                            ].reset_index(drop=True)["table"][0]
                        ).values()
                    )[0]
                    destination_list = list(
                        eval(
                            kpi_master[
                                kpi_master["kpicode"] == kpi_list[1]
                            ].reset_index(drop=True)["table"][0]
                        ).values()
                    )[0]
                    join_condition = cross_validation(
                        source_list, source_tab, destination_list, destination_tab
                    )
                    join_condtion_dict = kpi_eval(kpi_list, mod_name)
                    where_condition = inner_filter
                    for var in kpi_list:
                        where_condition = re.sub(
                            var + "(?!\d+)",
                            str(list(join_condtion_dict[var].keys())[0])
                            + "_validated_table."
                            + str(list(join_condtion_dict[var].values())[0])
                            + " ",
                            where_condition,
                        )  # where_condition = where_condition.replace(
                        #     i,
                        #     str(list(join_condtion_dict[i].keys())[0])
                        #     + "_validated_table."
                        #     + str(list(join_condtion_dict[i].values())[0]),
                        # )
                    take_action(
                        stage,
                        rule_name,
                        rule_display_name,
                        rule_description,
                        where_condition,
                        rule_action,
                        value_tobereplaced,
                        temp_tab1,
                        kpi_list,
                        source_list,
                        destination_list,
                        join_condition,
                        pulltype_flag,
                        mod_name,
                    )
                elif table == temp_tab2:
                    print("\ntable:", temp_tab1, "\ntemp_tab2:", temp_tab2)
                    source_tab = temp_tab2 + "_validated_table"
                    destination_tab = temp_tab1 + "_validated_table"
                    source_list = list(
                        eval(
                            kpi_master[
                                kpi_master["kpicode"] == kpi_list[1]
                            ].reset_index(drop=True)["table"][0]
                        ).values()
                    )[0]
                    destination_list = list(
                        eval(
                            kpi_master[
                                kpi_master["kpicode"] == kpi_list[0]
                            ].reset_index(drop=True)["table"][0]
                        ).values()
                    )[0]
                    join_condition = cross_validation(
                        source_list, source_tab, destination_list, destination_tab
                    )
                    join_condtion_dict = kpi_eval(kpi_list, mod_name)
                    where_condition = inner_filter
                    for var in kpi_list:
                        where_condition = re.sub(
                            var + "(?!\d+)",
                            str(list(join_condtion_dict[var].keys())[0])
                            + "_validated_table."
                            + str(list(join_condtion_dict[var].values())[0])
                            + " ",
                            where_condition,
                        )  # where_condition = where_condition.replace(
                        #     i,
                        #     str(list(join_condtion_dict[i].keys())[0])
                        #     + "_validated_table."
                        #     + str(list(join_condtion_dict[i].values())[0]),
                        # )
                    take_action(
                        stage,
                        rule_name,
                        rule_display_name,
                        rule_description,
                        where_condition,
                        rule_action,
                        value_tobereplaced,
                        table,
                        kpi_list,
                        source_list,
                        destination_list,
                        join_condition,
                        pulltype_flag,
                        mod_name,
                    )
                else:
                    print("Destination table is not found")

    elif len(rule_group) == 0:
        print("No QC'S for this table")

    elif check_table(table) == 0:
        rule_display_name = "Empty Table check"
        rule_description = "checks if the table is empty or not"
        summary_report(
            table,
            "EMPTY",
            stage,
            "data_check",
            rule_display_name,
            rule_description,
            rule_action,
            source_tab,
            destination_tab,
            join_condition,
        )


def get_custom_queries(table_name: str, table_location: str, mod_name: str):

    import urllib
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    from google.cloud import storage
    import json

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

    if mod_name == "derived_table":
        qc_query = f"""SELECT * FROM (
                    select
                    qc_name , qc_type, qc_description                  
                        from data_platform.custom_qc qc
                        left join data_platform.table_info ti
                        on qc.table_id=ti.table_id
                        where table_name='{table_name}' and qc_type<>'REPORT' and table_location='{table_location.lower()}') as a """

    elif mod_name == "post_derived_table":
        qc_query = f"""SELECT * FROM (
                    select
                    qc_name , qc_type, qc_description                  
                        from data_platform.custom_qc qc
                        left join data_platform.table_info ti
                        on qc.table_id=ti.table_id
                        where table_name='{table_name}' and  qc_type='REPORT' and table_location='{table_location.lower()}') as a """

    else:
        qc_query = f"""SELECT * FROM (
                    select
                    qc_name , qc_type, qc_description                   
                        from data_platform.custom_qc qc
                        left join data_platform.table_info ti
                        on qc.table_id=ti.table_id
                        where table_name='{table_name}' and table_location='{table_location.lower()}') as a """

    qc_data = pd.read_sql(qc_query, db_engine)
    qc_list = list(qc_data.qc_name)
    if qc_list:
        qc_type_dict = dict(zip(qc_data.qc_name, qc_data.qc_type))
        qc_description_dict = dict(zip(qc_data.qc_name, qc_data.qc_description))
        client = storage.Client()
        bucket = client.get_bucket(Variable.get("gcs_bucket"))
        blob = bucket.get_blob("custom_qc/queries.json")
        file = json.loads(blob.download_as_text(encoding="utf-8"))
        qc_dict = {key: file[key] for key in qc_list}
        return qc_dict, qc_type_dict, qc_description_dict
    else:
        return  {}, {}, {}


def update_anomaly_summary(qc_name, qc_type, table_name, entity, module_name, qc_desc):
    from google.cloud.exceptions import NotFound
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import (
        generate_create_table_ddl,
        generate_insert_ddl,
    )
    from database_utility.utils import datetime_const

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    if qc_type == 'PAUSE':
        summary_table = wc._get_complete_table_name(f"{entity}_anomaly_summary", False)
        delete_pause_action_query = f"""delete from {summary_table} 
                                        where action='{qc_type}'""" 
        query_job = wc.execute_query(delete_pause_action_query,False)
        print("Deleted previous pause action rows:", delete_pause_action_query)

    try:
        wc.table_exists(f"{entity}_anomaly_summary", True)
    except NotFound:
        print("Table {} is not found.".format(f"{entity}_anomaly_summary"))
        table_schema = {
            "table": "string",
            "module": "string",
            "rule": "string",
            "rule_display_name": "string",
            "rule_description": "string",
            "action": "string",
            "no_original_rows": "int",
            "affected_rows": "int",
            "affected_rows_percent": "float",
            "status": "string",
            "insertion_date": "datetime",
        }

        query = generate_create_table_ddl(
            warehouse, wc._get_complete_table_name(f"{entity}_anomaly_summary", False), table_schema
        )
        rows = wc.execute_query(query, True)

    data_dict = {
        "table": table_name,
        "module": module_name,
        "rule": qc_name,
        "rule_display_name": qc_name,
        "rule_description": qc_desc,
        "action": qc_type,
        "no_original_rows": eval(Variable.get(f"{qc_name}_total_rows")),
        "affected_rows": eval(Variable.get(f"{qc_name}_affected_rows")),
        "affected_rows_percent": eval(Variable.get(f"{qc_name}_loss_percent")),
        "status": Variable.get(f"{qc_name}_status", "Fail"),
        "insertion_date": datetime_const(warehouse),
    }
    query = generate_insert_ddl(warehouse, wc._get_complete_table_name(f"{entity}_anomaly_summary", False), data_dict)
    wc.execute_query(query, True)


def update_anomaly_table(
    qc_name, qc_query, qc_type, table_name, entity, module_name, description
):

    import pandas as pd
    from airflow.models import Variable
    from google.cloud.exceptions import NotFound
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    table_column_names = wc._get_table_columns(table_name)
    table_column_names = ",".join([f"{str(i)}" for i in table_column_names])
    final_column_names = table_column_names + ",QC,action,insertion_date"
    query = f"""{qc_query}"""
    rows = wc.execute_query(query, True)
    row_count_value = wc._get_row_count(table_name)
    Variable.set(f"{qc_name}_total_rows", row_count_value)
    Variable.set(f"{qc_name}_qc_rows", rows.shape[0])
    if rows.shape[0] == 1 and rows.shape[1] == 1:
        qc_df = rows
        Variable.set(f"{qc_name}_status", qc_df.loc[0, "status"])
        Variable.set(f"{qc_name}_affected_rows", 0)
    else:
        try:
            wc.table_exists(f"{entity}_anomaly", True)
            query = f"""insert into {wc._get_complete_table_name(entity+"_anomaly", False)} ({final_column_names})
            select {final_column_names}
            from ({qc_query})
            where action='{qc_type}' """
            print(query)
            rows = wc.execute_query(query, False)
            if rows == 0:
                print("No rows were added to the anomaly table")
            Variable.set(f"{qc_name}_affected_rows", rows.total_rows)
            if rows.total_rows > 1:
                Variable.set(f"{qc_name}_status", "Fail")
            else:
                Variable.set(f"{qc_name}_status", "Pass")

        except NotFound:
            query = f"""create table {Variable.get('gcp_project')}.{Variable.get('dataset')}.{entity}_anomaly as 
                (select {final_column_names}
                from ({qc_query})
                where action='{qc_type}')"""
            rows = wc.execute_query(query, False)
            print(query)
            if rows == 0:
                print("No rows were added to the anomaly table")
            Variable.set(f"{qc_name}_affected_rows", rows.total_rows)
            if rows > 1:
                Variable.set(f"{qc_name}_status", "Fail")
            else:
                Variable.set(f"{qc_name}_status", "Pass")

    try:
        loss_percent = eval(Variable.get(f"{qc_name}_affected_rows")) / eval(
            Variable.get(f"{qc_name}_total_rows")
        )
    except ZeroDivisionError:
        loss_percent = 0

    Variable.set(f"{qc_name}_loss_percent", loss_percent)

    return update_anomaly_summary(
        qc_name, qc_type, table_name, entity, module_name, description
    )


def check_generic_schema_mapping(entity_name):

    import pandas as pd
    from airflow.models import Variable
    import urllib
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

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

    generic_master_mapping_unfiltered = pd.read_sql_table(
        "generic_master_mapping", db_engine, schema=Variable.get("PGXSCHEMA")
    )
    generic_schema_mapping_dict = generic_master_mapping_unfiltered.set_index(
        "source_table"
    )["generic_mapping_table"].to_dict()

    if entity_name not in generic_schema_mapping_dict:
        return 0, {}

    generic_schema_mapping_table = pd.read_sql_table(
        f"{generic_schema_mapping_dict[entity_name]}",
        db_engine,
        schema=Variable.get("PGXSCHEMA"),
    ).to_dict()

    return 1, generic_schema_mapping_table


def generic_qc(entity_name):

    import logging
    import pandas as pd
    from airflow.models import Variable
    from airflow.exceptions import AirflowFailException
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    generic_qc_flag, mapping_table = check_generic_schema_mapping(
        entity_name
    )  # check if schema mapping table exist and retrun the flag along with the table as a dict
    if generic_qc_flag == 0:
        print(
            "Skipping generic mapping check since generic_schema_mapping_table is not available"
        )
        pass
    elif generic_qc_flag == 1:
        mapping_data = pd.DataFrame.from_dict(mapping_table)

        null_check = mapping_data[
            (mapping_data["is_null_allowed"] == False)
        ].source_column_name.to_list()  # stores the columns where null is not allowed

        null_cols = []
        for i in null_check:
            null_cols.append(f"{i} is null ")
        null_check_result_list = []

        if len(null_check) > 0:

            null_check_case_statement = ""
            for col in null_check:
                null_check_case_statement += f"WHEN {col} is NULL THEN '{col}' "

            null_check_where_statement = ""
            for col in null_check:
                null_check_where_statement += f" {col} IS NULL OR"

            # query to find the columns where null values are found
            null_check_query = f"""
                SELECT ARRAY_AGG(column_name) AS null_columns FROM (
                SELECT
                CASE
                    {null_check_case_statement}
                    
                    ELSE NULL
                END AS column_name
                FROM
                {wc._get_complete_table_name(entity_name + "_validated_table", False)}
                WHERE
                {null_check_where_statement[:-2]}
                group by column_name
                )
            """
        else:  # if nulls are allowed for all the columns
            null_check_query = f"""
                SELECT
                    NULL AS null_columns
            """

        level_cols = mapping_data[
            mapping_data["unique_by"] == True
        ].source_column_name.to_list()  # stores the columns at which the level of the table is defined

        level_cols_const = ",".join(level_cols)

        # query returns
        # 'null_columns' which holds the list of columns where nulls are found
        # 'duplicate_flag' which holds the flag = 1 if duplicates are found and 0 otherwise
        query = f"""
                with null_check as
                (
                {null_check_query}
                ),
                duplicate_check as(
                SELECT
                CASE
                    WHEN EXISTS (
                        SELECT
                {level_cols_const}
                FROM
                {wc._get_complete_table_name(entity_name + "_validated_table", False)}
                GROUP BY
                {level_cols_const}
                HAVING
                COUNT(*) > 1
                    )
                    THEN 1
                    ELSE 0
                END AS duplicate_flag
                )
                select null_columns, duplicate_flag from null_check cross join duplicate_check 
        """
        df = wc.execute_query(query, True)

        # Extract the list from the DataFrame
        if len(null_check) > 0:
            null_check_result_list = df["null_columns"].iloc[0]
        else:
            null_check_result_list = []
        duplicate_check_flag = df["duplicate_flag"].iloc[0]

        null_check_result_list_str = ", ".join(null_check_result_list)

        if len(null_check_result_list) == 0 and duplicate_check_flag == 0:
            print("Passed the null value check and the duplicate check")
            logging.info("Passed the null value check and the duplicate check")
        elif len(null_check_result_list) > 0 and duplicate_check_flag == 0:
            logging.info(
                f"""
                            Passed the duplicate check 
                            but null values found in columns {null_check_result_list_str}
                         """
            )
            raise AirflowFailException(
                f"""
                                    Passed the duplicate check 
                                    but null values found in columns {null_check_result_list_str}
                                    """
            )
        elif len(null_check_result_list) == 0 and duplicate_check_flag == 1:
            logging.info(
                f"""
                            Passed the null value check 
                            but duplicates found on level {level_cols_const}
                         """
            )
            raise AirflowFailException(
                f"""
                                    Passed the null value check 
                                    but duplicates found on level {level_cols_const}
                                    """
            )
        else:
            logging.info(
                f"""
                            Null values found in columns {null_check_result_list_str}
                            and duplicates found on level {level_cols_const}
                         """
            )
            raise AirflowFailException(
                f"""
                                        Null values found in columns {null_check_result_list_str}
                                        and duplicates found on level {level_cols_const}
                                    """
            )


def on_success_qc_callback(
    tab_name: str, entity_name: str, tab_location: str, mod_name: str
):
    check_flag = False
    from airflow.exceptions import AirflowFailException
    from airflow.models import Variable
    from database_utility.GenericDatabaseConnector import WarehouseConnector

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    if tab_name in eval(
        Variable.get("qc_entities_and_table_list")
    ) or entity_name in eval(Variable.get("qc_entities_and_table_list")):
        qc_dict, qc_type_dict, qc_description_dict = get_custom_queries(
            tab_name, tab_location, mod_name
        )

        # calls qc bot
        print("\n\nQC BOT CHECK FOR TABLE:", entity_name)
        [
            query_formation(arg, entity_name, mod_name)
            for arg in ["table_specific", "cross_validation"]
        ]

        # calls generic check module
        if mod_name != "post_derived_table":
            generic_qc(entity_name)

        # calls custom qc module
        if qc_dict and Variable.get("ingestion_type") == "periodic":
            inputs = []
            for name, query in qc_dict.items():
                action = qc_type_dict[name]
                description = qc_description_dict[name]
                update_anomaly_table(
                    name, query, action, tab_name, entity_name, mod_name, description
                )

            # with multiprocessing.Pool() as pool:
            #     results = pool.starmap(update_anomaly_table,inputs)

        # check for summary table if pause action is present and status is Fail
        try:
            wc.table_exists(f"{entity_name}_anomaly_summary", True)
            query = f"""
                    select *
                    from {wc._get_complete_table_name(entity_name + "_anomaly_summary",False)}
                    where action='PAUSE' and status ='Fail' and module<>"validation" """
            print(query)
            rows = wc.execute_query(query, False)
            if rows > 0:
                #raise AirflowFailException("QC check failed. Please check the summary table")
                check_flag = True
            else:
                return entity_name
        except:
            print(f"{entity_name}_anomaly_summary doesnt exist")
        if check_flag:
            raise AirflowFailException("QC check failed. Please check the summary table")
    else:
        return entity_name


def qc_helper(tup):
    return on_success_qc_callback(tup[0], tup[1], tup[2], tup[3])


def clear_qc_completion_table_list():
    from airflow.models import Variable

    Variable.set("qc_completion_table_list", "[]")


def post_derived_table_qc_run():
    # call for every derived table async on_success_qc_callback
    from airflow.models import Variable
    from concurrent.futures import ThreadPoolExecutor
    from airflow.exceptions import AirflowFailException

    if eval(Variable.get("qc_completion_table_list")):
        tables = eval(Variable.get("qc_completion_table_list"))
    else:
        tables = eval(Variable.get("der_tabs_names_lst"))
    mod_name = "post_derived_table"
    tab_location = Variable.get("warehouse").lower()
    arguements = []
    for tab in tables:
        arguements.append((f"{tab}_validated_table", tab, tab_location, mod_name))

    try:
        task_list = []
        result = []
        with ThreadPoolExecutor() as executor:
            task_list = executor.map(qc_helper, arguements)
            for r in task_list:
                result.append(r)
    except Exception as e:
        raise AirflowFailException(f"ERROR! {e}")
    finally:
        Variable.set("qc_completion_table_list", result)
    return result
