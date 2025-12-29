import pandas as pd

from dataflow_options.utils import Logger


log = Logger(__name__)


class DagTrigger:

    def __init__(self, trigger_type, poke_interval:int=300, timeout:int=18000, mode:str='reschedule', trigger_mapping_table='generic_trigger_mapping') -> None:
        self.trigger_type = trigger_type
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.mode = mode
        self.trigger_mapping_table = trigger_mapping_table
        
    def getTrigger(self):
        from airflow_options.notifications.trigger_notifications import send_trigger_success_notification, send_trigger_failure_notification
        from airflow.models import Variable
        from airflow_options.utils import get_trigger_mapping_from_db
        from google.cloud import bigquery
        from airflow.operators.dummy_operator import DummyOperator
        from airflow_custom_operators.sftp_sensor import SftpTriggerFileSensor
        from airflow_custom_operators.db_sensor import MSSQLSensor
        from airflow_custom_operators.gcs_sensor import GcsSensor
        from airflow_custom_operators.snowflake_sensor import SnowflakeSensor
        from database_utility.GenericDatabaseConnector import WarehouseConnector

        warehouse = Variable.get("warehouse")
        warehouse_kwargs = Variable.get("warehouse_kwargs",default_var='{}')
        wc = WarehouseConnector(warehouse, warehouse_kwargs)
        self.views = eval(Variable.get('views', default_var='[]'))
        self.on_success_callback = send_trigger_success_notification if eval(Variable.get('send_trigger_success_notification',"None")) else None
        self.on_failure_callback = send_trigger_failure_notification if eval(Variable.get('send_trigger_failure_notification',"None")) else None
        if self.trigger_type is None or self.trigger_type=='None':
            self.trigger_mapping_df = pd.DataFrame()
        else:
            # Enter this check only when we want to read config from self service UI
            if eval(Variable.get("read_from_db_flag", default_var="False")):
                self.trigger_mapping_df = get_trigger_mapping_from_db()
            else:
                self.trigger_mapping_df = wc.execute_query(f"""
                                                            SELECT * FROM {wc._get_complete_table_name(self.trigger_mapping_table, True)}
                                                            """, True).drop_duplicates()
        # Get current views from the mapping DataFrame
        current_views = self.trigger_mapping_df['view'].tolist()

        # Find views that are absent in the mapping
        absent_view_trigger_mapping = set(self.views) - set(current_views)

        if absent_view_trigger_mapping:  # Corrected condition check
            # Delete variables that are not present in the mapping
            for var in absent_view_trigger_mapping:
                try:
                    deletion_var = var + '_dirs_with_trigger_file'
                    Variable.delete(deletion_var)
                    log.info(f"Deleted variable: {deletion_var}")
                except KeyError:
                    log.info(f"Variable {deletion_var} not found.")    


        if self.trigger_type is None or self.trigger_type=='None':
            return DummyOperator(
                task_id="dummy_trigger"
            )
        elif self.trigger_type.lower()=='sftp':
            return SftpTriggerFileSensor(
                task_id="TRIGGER",
                poke_interval=self.poke_interval,
                timeout=self.timeout,   #5*60*60, #Fail after 5hrs
                mode=self.mode,
                trigger_mapping_table=self.trigger_mapping_table,
                trigger_mapping_df=self.trigger_mapping_df,
                on_success_callback=self.on_success_callback,
                on_failure_callback=self.on_failure_callback
            )
        elif self.trigger_type.lower()=='mssql':
            return MSSQLSensor(
                task_id="TRIGGER",
                poke_interval=self.poke_interval,
                timeout=self.timeout,   #5*60*60, #Fail after 5hrs
                mode=self.mode,
                trigger_mapping_table=self.trigger_mapping_table,
                trigger_mapping_df=self.trigger_mapping_df,
                on_success_callback=self.on_success_callback,
                on_failure_callback=self.on_failure_callback
            )
        elif self.trigger_type.lower()=='gcs':
            return GcsSensor(
                task_id="TRIGGER",
                poke_interval=self.poke_interval,
                timeout=self.timeout,   #5*60*60, #Fail after 5hrs
                mode=self.mode,
                trigger_mapping_table=self.trigger_mapping_table,
                trigger_mapping_df=self.trigger_mapping_df,
                on_success_callback=self.on_success_callback,
                on_failure_callback=self.on_failure_callback 
            )
        elif self.trigger_type.lower()=='snowflake':
            return SnowflakeSensor(
                task_id="TRIGGER",
                poke_interval=self.poke_interval,
                timeout=self.timeout,   #5*60*60, #Fail after 5hrs
                mode=self.mode,
                trigger_mapping_table=self.trigger_mapping_table,
                trigger_mapping_df=self.trigger_mapping_df,
                on_success_callback=self.on_success_callback,
                on_failure_callback=self.on_failure_callback 
            )
        else:
            raise Exception(f"{self.trigger_type} is not supported")
    
    
