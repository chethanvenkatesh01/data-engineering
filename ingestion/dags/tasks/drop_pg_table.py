def drop_table(hierarchy):

    import psycopg2
    from airflow import models

    user = models.Variable.get('fmt_refresh_PGXUSER')
    password = models.Variable.get('fmt_refresh_PGXPASSWORD')
    host = models.Variable.get('fmt_refresh_PGXHOST')
    port = models.Variable.get('fmt_refresh_PGXPORT')
    database = models.Variable.get('fmt_refresh_PGXDATABASE')
    schema = models.Variable.get('fmt_refresh_PGXSCHEMA')

    connection = psycopg2.connect(user=user,
                                password=password,
                                host=host,
                                port=port,
                                database=database)
    cursor = connection.cursor()
    postgresql_drop_query = f"drop table IF EXISTS {schema}.master_aggregate_{hierarchy}"
    cursor.execute(postgresql_drop_query)

    postgresql_rename_query = f"ALTER TABLE {schema}.master_aggregate_{hierarchy}_temp RENAME TO master_aggregate_{hierarchy}"
    cursor.execute(postgresql_rename_query)

    connection.commit()

    if connection:
        cursor.close()
        connection.close()