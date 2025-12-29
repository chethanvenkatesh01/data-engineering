from typing import Dict
from airflow.sensors.base import BaseSensorOperator
from dataflow_options.utils import Logger

import pandas as pd


log = Logger(__name__)

def sqlalchemy_url_builder(database_type, host, username, password, database_name, port=None, dialect=None, driver=None):
    from sqlalchemy.engine.url import URL
    if database_type.lower() in ['sqlserver','mssql','sql server']:
        port = "1433" if port is None else port
        dialect = "pymssql" if dialect is None else dialect
        url_object = URL(f"mssql+{dialect}", username=username, password=password, host=host, port=port, database=database_name)
        return url_object

    else:
        raise Exception(f"{database_type} is not supported")       


class MSSQLSensor(BaseSensorOperator):
    def __init__(self, *, trigger_mapping_table:str='generic_trigger_mapping', trigger_mapping_df:pd.DataFrame=None, **kwargs) -> None:
        self.trigger_mapping_table=trigger_mapping_table
        self.trigger_mapping_df=trigger_mapping_df
        super().__init__(**kwargs)
    
    def poke(self, context: Dict):
        from airflow.models import Variable
        from google.cloud import bigquery
        from collections import defaultdict
        from concurrent.futures import ThreadPoolExecutor, wait
        from sqlalchemy.engine.result import Row as RowProxy
        from airflow_options.utils import get_source_config_value, get_trigger_status, get_trigger_mapping_from_db
        import pytz
        from datetime import datetime
        from airflow_options.notifications.trigger_notifications import send_trigger_warning_mail
        from pretty_html_table import build_table
        from database_utility.GenericDatabaseConnector import WarehouseConnector

        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs")
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        task_id = context['task'].task_id
        #config_query_map = self.trigger_config_query_mapping_df.to_dict('records')
        failed_view_trigger_map = defaultdict(list)

        self.views = eval(Variable.get('views', default_var='[]'))
        self.view_trigger_status_mapping:dict = eval(Variable.get(f"view_trigger_status_mapping", default_var="{}"))
        if self.trigger_mapping_df is None:
            self.trigger_mapping_table = self.trigger_mapping_table
            # Enter this check only when we want to read config from self service UI
            if eval(Variable.get("read_from_db_flag", default_var="False")):
                self.trigger_mapping_df = get_trigger_mapping_from_db()
            else:
                self.trigger_mapping_df = wc.execute_query(f"""
                                                            SELECT * FROM {wc._get_complete_table_name(self.trigger_mapping_table, True)}
                                                            """, True).drop_duplicates()
            self.trigger_mapping_df = self.trigger_mapping_df[self.trigger_mapping_df['view'].isin(self.views)]
        else:
            self.trigger_mapping_df = self.trigger_mapping_df[self.trigger_mapping_df['view'].isin(self.views)]
        
        self.view_trigger_mandatory_mapping:dict = self.trigger_mapping_df.set_index('view').to_dict()['is_mandatory']
        Variable.set(f"view_trigger_mandatory_mapping", self.view_trigger_mandatory_mapping)
        # Set trigger status as True for the views which are not present in trigger_mapping_df
        for view in self.view_trigger_status_mapping:
            if view not in self.trigger_mapping_df['view'].tolist():
                self.view_trigger_status_mapping[view]=True
        self.source_configs = {}
        for src_conf in list(self.trigger_mapping_df['source_config'].unique()):
            self.source_configs[src_conf] = get_source_config_value(src_conf)

        futures = []
        view_list = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for row in self.trigger_mapping_df.to_dict('records'):
                view_name = row['view']
                if view_name in self.views:
                    config = self.source_configs[row['source_config']]
                    connector = row['connector']
                    trigger_rule = row['trigger_rule']
                    trigger_query = row['trigger_query']
                    trigger_file = row['trigger_file']
                    
                    view_list.append(view_name)
                    futures.append(executor.submit(get_trigger_status, view_name, connector, config, trigger_rule, trigger_query, trigger_file))
            
            wait(futures)
            log.info(f"Fetched the trigger status for all views")

            for idx, future in enumerate(futures):
                result = future.result()
                #log.info(f"Type of result[0]: {type(result[0])}")
                log.info(f"{idx}:{view_list[idx]} - {result}")
                if len(result) == 0:
                    self.view_trigger_status_mapping[view_list[idx]] = True
                else:
                    self.view_trigger_status_mapping[view_list[idx]] = False
                    failed_view_trigger_map[view_list[idx]] = [row[0] if isinstance(row, tuple) or isinstance(row, list) or isinstance(row, RowProxy) 
                                                               else row for row in result]
        

        Variable.set(f"view_trigger_status_mapping", self.view_trigger_status_mapping)
        log.info(f"{failed_view_trigger_map}")
        timezone = pytz.timezone(Variable.get('dag_timezone', default_var='UTC'))
        curr_datetime = datetime.now(timezone)
        trigger_warning_time = datetime.strptime(Variable.get("trigger_warning_time", default_var="00:00"),"%H:%M").time()
        trigger_cutoff_time = datetime.strptime(Variable.get("trigger_cutoff_time", default_var="00:00"),"%H:%M").time()
        trigger_warning_datetime = timezone.localize(datetime.combine(curr_datetime.date(), trigger_warning_time))
        trigger_cutoff_datetime = timezone.localize(datetime.combine(curr_datetime.date(), trigger_cutoff_time))

        if Variable.get('trigger_warning_time')!="" and eval(Variable.get('send_trigger_warning_mail', default_var="False")):
            if curr_datetime >= trigger_warning_datetime and len(failed_view_trigger_map)>0:
                warning_mail_validations = {'view':[],'missing_triggers':[],'is_trigger_mandatory':[]}
                for k,v in failed_view_trigger_map.items():
                    warning_mail_validations['view'].append(k)
                    warning_mail_validations['missing_triggers'].append(",".join(v))
                    warning_mail_validations['is_trigger_mandatory'].append(self.view_trigger_mandatory_mapping.get(k))
                warning_mail_validation_table = build_table(
                    pd.DataFrame(warning_mail_validations), "blue_light", font_size="10px", text_align="center"
                ).replace('"', "'")
                Variable.set(f"{task_id}_warning_mail_validation_table", warning_mail_validation_table)
                send_trigger_warning_mail(context)
                Variable.set('send_trigger_warning_mail', "False")
        
        mandatory_triggers = [view for view,is_mandatory in self.view_trigger_mandatory_mapping.items() if is_mandatory]
        received_all_mandatory_triggers = True
        for trigger in mandatory_triggers:
            if trigger in failed_view_trigger_map:
                log.error(f"Didn't receive all mandatory triggers")
                received_all_mandatory_triggers = False
        
        if received_all_mandatory_triggers:
            # Trigger is successful
            trigger_validations = {'view':[],'trigger_received':[],'is_trigger_mandatory':[]}
            for view, trg_status in self.view_trigger_status_mapping.items():
                trigger_validations['view'].append(view)
                trigger_validations['trigger_received'].append(trg_status)
                trigger_validations['is_trigger_mandatory'].append(self.view_trigger_mandatory_mapping.get(view))
            trigger_validations_table = build_table(
                pd.DataFrame(trigger_validations), "blue_light", font_size="10px", text_align="center"
            ).replace('"', "'")
            Variable.set(f"{task_id}_trigger_validation_table", trigger_validations_table)
            return True
        else:
            # Trigger failed
            failure_mail_validations = {'view':[],'missing_triggers':[],'is_trigger_mandatory':[]}
            received_all_mandatory_triggers = True
            for k,v in failed_view_trigger_map.items():
                failure_mail_validations['view'].append(k)
                failure_mail_validations['missing_triggers'].append(",".join(v))
                failure_mail_validations['is_trigger_mandatory'].append(self.view_trigger_mandatory_mapping.get(k))
                if self.view_trigger_mandatory_mapping.get(k):
                    received_all_mandatory_triggers = False
            failure_mail_validation_table = build_table(
                pd.DataFrame(failure_mail_validations), "blue_light", font_size="10px", text_align="center"
            ).replace('"', "'")
            Variable.set(f"{task_id}_failure_mail_validation_table", failure_mail_validation_table)
            Variable.set(f"received_all_mandatory_triggers", received_all_mandatory_triggers)
            return False



    

        
        

