import pymysql
import psycopg2
import pymssql 

CLIENT_SCHEMA                  = 'int_ia'
QUERY_SIZE                     = 700000

SCHEMA_QUERY = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}';"

CONFIG_MAPPING = {
    'MYSQL'   : pymysql,
    'POSTGRES': psycopg2,
    'MSSQL'   : pymssql
}

SCHEMA_MAPPING = {
  'nvarchar': 'STRING',
  'int'     : 'INTEGER',
  'numeric' : 'FLOAT',
  'varchar' : 'STRING',
  'bigint'  : 'INTEGER',
  'datetime': 'TIMESTAMP',
  'time'    : 'TIME'
}
