def connect_to_db_and_execute_query(
    db_host,
    db_port,
    db_database,
    db_user,
    db_password,
    query,
):
    import pandas as pd
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    conn_string = (
        "postgresql://"
        + db_user
        + ":"
        + db_password
        + "@"
        + db_host
        + ":"
        + db_port
        + "/"
        + db_database
    )
    db_engine = create_engine(conn_string, poolclass=NullPool)
    result = pd.read_sql(query, db_engine)
    return result
