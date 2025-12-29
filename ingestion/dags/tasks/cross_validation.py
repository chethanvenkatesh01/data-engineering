async def generate_cols():
    import asyncpg
    from airflow import models
    connection = await asyncpg.connect(user=models.Variable.get('PGXUSER'), host=models.Variable.get('PGXHOST'), password=models.Variable.get('PGXPASSWORD'), database=models.Variable.get('PGXDATABASE'), port=models.Variable.get('PGXPORT'))

    cols = await connection.fetch(f"select destination_table, cross_validations from {models.Variable.get('PGXSCHEMA')}.generic_master_mapping \
                where cross_validations is not null")
    await connection.close()
    cols = [dict(col) for col in cols]
    return cols 

async def generate_table_cols():
    import asyncio
    result = await asyncio.gather(generate_cols())
    return result

def generate_cross_validations_tables():
    import asyncio
    from airflow import models
    cols = asyncio.run(generate_table_cols())
    tables = [item['destination_table'] for item in cols[0]]
    cross_validations = [item['cross_validations'] for item in cols[0]]
    models.Variable.set('tables', tables)
    models.Variable.set('cross_validations', cross_validations)


async def generate_source_cross_validations(name):
    import asyncpg
    from airflow import models
    connection = await asyncpg.connect(user=models.Variable.get('PGXUSER'), host=models.Variable.get('PGXHOST'), password=models.Variable.get('PGXPASSWORD'), database=models.Variable.get('PGXDATABASE'), port=models.Variable.get('PGXPORT'))

    cols = await connection.fetch(f"select generic_column_name from {models.Variable.get('PGXSCHEMA')}.{name}_generic_schema_mapping where unique_by is true")
    await connection.close()
    cols = [dict(col) for col in cols]
    return cols 

async def generate_source_table_cross_validations(name):
    import asyncio
    result = await asyncio.gather(generate_source_cross_validations(name))
    return result

def generate_source_cross_validations_target(name):
    import asyncio
    cols = asyncio.run(generate_source_table_cross_validations(name))
    columns = [item['generic_column_name'] for item in cols[0]]
    return columns

async def generate_target_cross_validations(name):
    import asyncpg
    from airflow import models
    connection = await asyncpg.connect(user=models.Variable.get('PGXUSER'), host=models.Variable.get('PGXHOST'), password=models.Variable.get('PGXPASSWORD'), database=models.Variable.get('PGXDATABASE'), port=models.Variable.get('PGXPORT'))

    cols = await connection.fetch(f"select generic_column_name from {models.Variable.get('PGXSCHEMA')}.{name}_generic_schema_mapping")
    await connection.close()
    cols = [dict(col) for col in cols]
    return cols 

async def generate_target_table_cross_validations(name):
    import asyncio
    result = await asyncio.gather(generate_target_cross_validations(name))
    return result

def generate_cross_validations_target(name):
    import asyncio
    cols = asyncio.run(generate_target_table_cross_validations(name))
    columns = [item['generic_column_name'] for item in cols[0]]
    return columns 

cross_validation_query = \
    """select s.*,'{3}' as mismatch_reason  from {0} s
    left join {1} t
    on s.{2} = t.{2}
    where t.{2} is null
    """