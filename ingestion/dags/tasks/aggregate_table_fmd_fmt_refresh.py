from constants import constant_fmd_to_fmt as constant

def get_aggregate_query(hierarchies, num):
    import pandas as pd
    from airflow import models
    import urllib
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    """
    Runs the aggregation
    """

    conn_string = (
        "postgresql://"
        + models.Variable.get("fmt_refresh_PGXUSER")
        + ":"
        + urllib.parse.quote(models.Variable.get("fmt_refresh_PGXPASSWORD"))
        + "@"
        + models.Variable.get("fmt_refresh_PGXHOST")
        + ":"
        + models.Variable.get("fmt_refresh_PGXPORT")
        + "/"
        + models.Variable.get("fmt_refresh_PGXDATABASE")
    )
    db_engine = create_engine(conn_string, poolclass=NullPool)

    cols_data = pd.read_sql(
        f"""select concat('{{ ', string_agg(temp,' , ') ,' }}') as col from (select concat( '"',entity,'"',  ' : ' , '"' ,STRING_AGG(gbq_formula, ' , '), '"')  as temp
                                from {models.Variable.get('fmt_refresh_PGXSCHEMA')}.{constant.fmd_name} where entity in ('aggregation') group by entity ) as a""",
        db_engine,
    )

    aggregation_period = models.Variable.get(
        "fmt_refresh_aggregation_period", default_var="fiscal_year_week"
    )
    time_hierarchy = [
        "date",
        "fiscal_year_week",
        "fiscal_bi_week",
        "fiscal_year_month",
        "fiscal_year_quarter",
        "fiscal_year",
    ]
    time_index = time_hierarchy.index(aggregation_period)
    updated_time_hierarchy = time_hierarchy[time_index:]

    special_clause = pd.read_sql(
        f"""select concat('{{ ', string_agg(temp,' , ') ,' }}') as col from (select concat( '"',column_name,'"',  ' : ' , '"' ,STRING_AGG(gbq_formula, ' , '), '"')  as temp
                                from {models.Variable.get('PGXSCHEMA')}.feature_metadata where entity in ('aggregation') and column_name  like '\_\_%%' group by column_name ) as a""",
        db_engine,
    )

    cols_dict = eval(cols_data.col.iloc[0].replace("\n", ""))
    special_clause_dict = eval(special_clause.col.iloc[0].replace("\n", ""))

    group_by_columns = hierarchies
    group_by_columns.extend(updated_time_hierarchy)

    query = f"""select {','.join(group_by_columns)},
                    {cols_dict['aggregation']}
                    from {models.Variable.get('fmt_refresh_gcp_project')}.{models.Variable.get('fmt_refresh_dataset')}.master_input_imputed_validated as quant_sold_agg
                    {special_clause_dict.get('__where_clause')}
                    group by {','.join(group_by_columns)}"""

    models.Variable.set(f"fmt_refresh_aggregation_query_{num}", query)
