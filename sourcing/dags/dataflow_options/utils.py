import apache_beam as beam 
from dataflow_options.constants import SCHEMA_MAPPING, CLIENT_SCHEMA, QUERY_SIZE
import re
import logging

pattern = re.compile('\W')

class SchemaString(beam.CombineFn):
    def create_accumulator(self):
        return [{'name': 'table_name', 'type': 'STRING'}]
    
    def add_input(self, accumulator, input):
        column = input[0]
        column_type = SCHEMA_MAPPING[input[1]] if input[1] in SCHEMA_MAPPING else input[1]
        accumulator.append({'name': column, 'type': column_type})
        return accumulator
    def merge_accumulators(self, accumulator):
        return accumulator

    def extract_output(self, accumulator):
        return {'fields': list(accumulator)[0]}

def row_count_query(view,filter=None, filter_param=None, pull_type=None):
  out = []
  sub_query = ""
  if pull_type == "incremental":
    sub_query = f"where {filter} > '{filter_param}'"
  query = f"""
      select count(*)
      from {CLIENT_SCHEMA}.{view} 
      {sub_query}
    """
  out.append(query)
  return out


def create_queries(view, count, table_dict, step=QUERY_SIZE, filter=None, filter_param=None, pull_type=None):
  out = []
  count = list(count)[0][0]
  column_list = table_dict["fields"][:]

  column_list_new = []
  for item in column_list:
    if item['name'] != 'table_name':
      if item['type'] == 'TIMESTAMP':
        column_list_new.append(f' LEFT(CONVERT(VARCHAR, {item["name"]} , 25),19) as {re.sub(pattern, "", item["name"])} ')
      elif item['type'] == 'TIME':
        column_list_new.append(f' convert(varchar,convert(time(6), {item["name"]})) as {re.sub(pattern, "", item["name"])} ')
      elif item['type'] == 'FLOAT':
        column_list_new.append(f' str({item["name"]},8,8)  as {re.sub(pattern, "", item["name"])} ')
      else :
        column_list_new.append(f' cast({item["name"]} as varchar) as {re.sub(pattern, "", item["name"])} ')
  
  where_clause = ""
  if pull_type == "incremental":
    where_clause = f"where {filter} > '{filter_param}'"
    
  for i in range(0, count, step):
    query = f"select '{view}' as table_name , \
                      {', '.join(column_list_new)}  " \
            f"from {CLIENT_SCHEMA}.{view} "\
            f"{where_clause}" \
            f"order by table_name " \
            f"offset {i} rows " \
            f"fetch next {step} rows only;"
    out.append(query)
    print(query)
  return out 


def fix_schema(schema_dict):
  column_list = schema_dict["fields"]
  new_list = [
    {
      "name": re.sub(pattern, "", item["name"]), 
      "type": item["type"]
    } 
    for item in column_list
  ]
  return str(new_list).replace("'",'"')



class Logger:

    def __init__(self, name, log_level='INFO') -> None:
        logging.basicConfig()
        if log_level == 'DEBUG':
            logging.root.setLevel(logging.DEBUG)
        elif log_level == 'WARNING':
            logging.root.setLevel(logging.WARNING)
        elif log_level == 'ERROR':
            logging.root.setLevel(logging.ERROR)
        else:
            logging.root.setLevel(logging.INFO)
        self.logger = logging.getLogger(name)
    
    def info(self, msg:str, exc_info:bool=False):
        self.logger.info(msg=msg, exc_info=exc_info)
    
    def debug(self, msg:str, exc_info:bool=False):
        self.logger.debug(msg=msg, exc_info=exc_info)
    
    def warn(self, msg:str, exc_info:bool=False):
        self.logger.warn(msg=msg, exc_info=exc_info)
    
    def error(self, msg:str, exc_info:bool=False):
        self.logger.error(msg=msg, exc_info=exc_info)
    


