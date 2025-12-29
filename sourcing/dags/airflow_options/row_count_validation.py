from dataflow_options.utils import Logger

log = Logger(__name__)

class Connectors:
    GCS = "GCS"
    MSSQL = "MSSQL"
    SNOWFLAKE = "snowflake"
    SFTP = "SFTP"

class RowCountValidation:

    def __init__(self):
        from airflow.models import Variable
        self.gcs_views = []
        self.mssql_views = []
        self.sftp_views = []
        self.snowflake_views = []
        self.views = eval(Variable.get('unique_views', default_var = '[]'))
        self.row_count_validation_info = {
            view:{
                "client_row_count":None,
                "sourced_row_count":None
            }
            for view in self.views
        }        

    def row_count_sourced(self):
        from airflow.models import Variable
        """ 
        fetch the sourced row count 
        """ 
        for view in self.views:
            row_count_before_load_job = int(Variable.get(f"{view}_param_num_rows_before_load_job", default_var="0"))
            row_count_after_load_job = int(Variable.get(f"{view}_param_num_rows_after_load_job", default_var="0"))

            if eval(Variable.get(f"{view}_param_replace", default_var='False')):
                sourced_row_count = row_count_after_load_job
            else:
                sourced_row_count = row_count_after_load_job - row_count_before_load_job

            self.row_count_validation_info[view] = {
                "client_row_count":None,
                "sourced_row_count":sourced_row_count
            }

    def row_count_gcs_connector(self, view):
        from airflow.models import Variable
        from google.cloud import storage
        import json
        """
        fetch the row count from client shared file from GCS
        """

        file_prefix = Variable.get(f"{view}_param_row_count_validation_info")
        dirs_with_trigger_file = eval(Variable.get(f'{view}_dirs_with_trigger_file', default_var = '[]'))
        row_count = 0

        # For each directory
        missing_files_logs = []
        for dir in dirs_with_trigger_file:

            # File name
            bucket = dir.split('/')[2]
            folder = dir.split(bucket)[1]
            folder = folder.lstrip('/')
            file = folder + file_prefix

            # Download the manifest file and count the rows
            try:
                gcs = storage.Client()
                bucket = gcs.get_bucket(bucket)
                blob = bucket.get_blob(file)
                manifestfile = json.loads(blob.download_as_text(encoding="utf-8"))
                manifestfile_lower_keys = {
                    key.lower() : value 
                    for key, value in manifestfile.items()
                }
                if manifestfile_lower_keys.get('row_count'):
                    row_count += manifestfile_lower_keys['row_count']
            except Exception as e:
                
                missing_files_logs.append(dir)
                log.info(f"The manifest file in ({dir}) not found. Error : {e}")
        
        # record the client_row_count and count_mismatch
        self.row_count_validation_info[view]["client_row_count"] = row_count

    def row_count_mssql_connector(self, view):
        from airflow.models import Variable
        from airflow_options.utils import get_row_count_for_views, get_source_config_value
        import pymssql
        """
        fetch the row count from client shared table from MSSQL
        """
        db_config = Variable.get(f'{view}_param_source_config')
        if db_config not in self.recoreded_mssql_connections:
            response = get_source_config_value(db_config)
            conn = pymssql.connect(
                response['server'], 
                response['user'], 
                response['password'], 
                response['database']
            )
            cursor = conn.cursor()
            cursor.execute(f'SELECT view, row_count, syncstartdatetime FROM {Variable.get(f"{view}_param_row_count_validation_info")}')
            # cursor.execute("select DEFINITIONGROUP as 'view', 1 as row_count, COMPAREDATETIME as syncstartdatetime from int_ia.VW_DIM_DATAMANAGEMENT_JOB vddj")
            data = cursor.fetchall()
            for row in data:
                try:
                    self.row_count_validation_info[row[0]]['client_row_count'] = row[1]
                except KeyError as err:
                    log.info(f"row count for {row[0]} was shared({row[1]}) but not being sourced.")
            self.recoreded_mssql_connections.append(db_config)

    def row_count_snowflake_connector(self, view):
        from airflow.models import Variable
        from airflow_options.utils import get_row_count_for_views, get_source_config_value
        import snowflake.connector
        """
        fetch the row count from client shared table from SNOWFLAKE
        """
        config = Variable.get(f'{view}_param_source_config')
        if config not in self.recorded_snowflake_configs:
            
            config = get_source_config_value(config)
            
            snowflake_conn = snowflake.connector.connect(
                user=config.get('username') or config.get('user'),
                password=config.get('password'),
                account=config.get('account'),
                warehouse=config.get('warehouse'),
                database=config.get('database'),
                schema=config.get('schema')
            )
            cur = snowflake_conn.cursor()
            # query = "select 'view_name' as view, 100 as row_count, item_active_tms as syncstartdatetime from web_item limit 1"
            query = f"select view, row_count, syncstartdatetime from {Variable.get(f'{view}_param_row_count_validation_info')}"
            cur.execute(query)
            query_id = cur.sfqid
            cur.get_results_from_sfqid(query_id)
            data = cur.fetchall()
            for row in data:
                try:
                    self.row_count_validation_info[row[0]]['client_row_count'] = row[1]
                except KeyError as err:
                    log.info(f"row count for {row[0]} was shared({row[1]}) but not being sourced.")
            self.recorded_snowflake_configs.append(config)
    
    def row_count_sftp_connector(self, view):
        from airflow.models import Variable
        from airflow_options.utils import get_row_count_for_views, get_source_config_value
        from ssh2.session import Session
        import socket
        from ssh2.sftp import LIBSSH2_FXF_READ, LIBSSH2_SFTP_S_IRUSR
        import json
        """
        fetch the row count from client shared file from SFTP
        """

        file_prefix = Variable.get(f"{view}_param_row_count_validation_info")
        dirs_with_trigger_file = eval(Variable.get(f'{view}_dirs_with_trigger_file', default_var = '[]'))
        
        if dirs_with_trigger_file:
            config = Variable.get(f'{view}_param_source_config')            
            config = get_source_config_value(config)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((config["host"], config.get("port", 22)))
            session = Session()
            session.handshake(sock)
            session.userauth_password(config["user"], config["password"])
            sftp = session.sftp_init()
            parent_dir = config["path"] + ("" if config["path"].endswith("/") else "/")

            # For each directory
            row_count = 0
            for dir in dirs_with_trigger_file:
                try:
                    sftp_file_handle = sftp.open(f"{parent_dir.rstrip('/')}{dir}{'/'}{file_prefix}",LIBSSH2_FXF_READ,LIBSSH2_SFTP_S_IRUSR)
                    manifestfile = sftp_file_handle.read()
                    manifestfile = json.loads(manifestfile[1])
                    row_count += manifestfile['row_count']
                except Exception as e:
                    log.info(f"The manifest file in ({dir}) not found. Error : {e}")

            # record the client_row_count and count_mismatch
            self.row_count_validation_info[view]['client_row_count'] = row_count


    def row_count_validation(self):
        from airflow_options.utils import get_row_count_for_views, get_source_config_value
        from airflow.models import Variable
        """
        If the flag is set then do row count validation
        """

        get_row_count_for_views()

        if eval(Variable.get("row_count_validation", default_var = "False")): 
            
            # Record the sourced row count
            self.row_count_sourced()

            # Record the client row count
            for view in self.views:
                
                # if the row_count_validation file is define for the view then
                if Variable.get(f"{view}_param_row_count_validation_info", default_var = "None") != "None":
                    view_connector = Variable.get(f"{view}_param_connector")
                    if view_connector == Connectors.GCS:
                        self.gcs_views.append(view)
                    elif view_connector == Connectors.MSSQL:
                        self.mssql_views.append(view)
                    elif view_connector == Connectors.SNOWFLAKE:
                        self.snowflake_views.append(view)
                    elif view_connector == Connectors.SFTP:
                        self.sftp_views.append(view)
                    else:
                        msg = f"Row count validation method for {view_connector} not found. Look up view '{view}'"
                        Variable.get("row_count_validation_info", msg)
                        raise Exception(msg)
            
            # For GCS
            if self.gcs_views:
                for view in self.gcs_views:
                    self.row_count_gcs_connector(view)
            
            # For MSSQL
            if self.mssql_views:
                self.recoreded_mssql_connections = []
                
                for view in self.mssql_views:
                    self.row_count_mssql_connector(view)
            
            # For snowflake
            if self.snowflake_views:
                self.recorded_snowflake_configs = []
                self.snowflake_table_name = "manifest_table"
                for view in self.snowflake_views:
                    self.row_count_snowflake_connector(view)

            # For SFTP
            if self.sftp_views:
                for view in self.sftp_views:
                    self.row_count_sftp_connector(view)

            Variable.set("row_count_validation_info", self.row_count_validation_info)
        
        else:
            log.info("Row count validation not initialized.")



