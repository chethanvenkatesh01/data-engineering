def create_df(hierarchy):
    import pandas as pd
    import psycopg2
    from airflow.models import Variable
    from google.cloud import bigquery
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)

    project_id = Variable.get('fmt_refresh_gcp_project')
    dataset = Variable.get('fmt_refresh_dataset')
    table_name = wc._get_complete_table_name(f'master_aggregate_{hierarchy}', False)
    query = f'''
    SELECT * FROM {table_name}
    '''
    data_df = wc.execute_query(query, True)
    client = bigquery.Client(project=project_id, location=Variable.get("bigquery_region", default_var="US"))
    dataset_ref = client.dataset(dataset, project=project_id)
    table_ref = dataset_ref.table(f'master_aggregate_{hierarchy}')
    table = client.get_table(table_ref)
    schema_bq = table.schema

    data_type_mapper = {
        'STRING' : 'TEXT',
        'INTEGER' : 'INTEGER',
        'FLOAT' : 'REAL',
        'BOOL' : 'BOOL'
    }
    schema_pg = []
    for schema in schema_bq:
        data_type = data_type_mapper[schema.field_type]
        schema_pg.append("{0} {1}".format(schema.name, data_type))
    print(str(schema_pg)[1:-1].replace("'",""))

    user = Variable.get('fmt_refresh_PGXUSER')
    password = Variable.get('fmt_refresh_PGXPASSWORD')
    host = Variable.get('fmt_refresh_PGXHOST')
    port = Variable.get('fmt_refresh_PGXPORT')
    database = Variable.get('fmt_refresh_PGXDATABASE')
    schema = Variable.get('fmt_refresh_PGXSCHEMA')

    conn = psycopg2.connect(user=user,
                                password=password,
                                host=host,
                                port=port,
                                database=database)

    cursor = conn.cursor()
    delete_table_query = f"""DROP TABLE IF EXISTS {schema}.master_aggregate_{hierarchy}_temp"""
    cursor.execute(delete_table_query)
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {schema}.master_aggregate_{hierarchy}_temp ({str(schema_pg)[1:-1].replace("'","")})"""

    cursor.execute(create_table_query)

    conn.commit()
    return data_df

def copy_gbq_to_pg(hierarchy):
    from urllib.parse import quote
    from airflow.models import Variable
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    data_df = create_df(hierarchy=hierarchy)

    conn_string = (
        "postgresql://"
        + Variable.get("fmt_refresh_PGXUSER")
        + ":"
        + quote(Variable.get("fmt_refresh_PGXPASSWORD"))
        + "@"
        + Variable.get("fmt_refresh_PGXHOST")
        + ":"
        + Variable.get("fmt_refresh_PGXPORT")
        + "/"
        + Variable.get("fmt_refresh_PGXDATABASE")
    )
    db_engine = create_engine(conn_string, poolclass=NullPool)
    with db_engine.connect():
        data_df.to_sql(f'master_aggregate_{hierarchy}_temp', con = db_engine, if_exists='replace', index = False, chunksize = 1000000)