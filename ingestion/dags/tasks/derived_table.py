from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

def wait_until(request_id, timeout, period ):

  import time
  import pandas as pd
  from airflow.models import Variable
  from database_utility.GenericDatabaseConnector import WarehouseConnector
  warehouse = Variable.get("warehouse")
  warehouse_kwargs = Variable.get("warehouse_kwargs")
  wc = WarehouseConnector(warehouse, warehouse_kwargs)
  table_name = wc._get_complete_table_name("derived_table_request_status",False)
  mustend = time.time() + timeout
  while time.time() < mustend:
    status= wc.execute_query(f"""select status from  {table_name} where request_id='{request_id}'""", True)
    if status['status'].iloc[0] in ['completed', 'failed']: 
        return status['status'].iloc[0]
    time.sleep(period)
  return "timed_out"


def trigger_python_script(task,arguments: dict):
    import json
    import requests
    from airflow.models import Variable
    from tasks import slack_integration as sl
    from tasks.utils import idtoken_from_metadata_server
    from constants.constant import ALLOWED_HTTP_REQUEST_TYPES, ALLOWED_HTTP_RESPONSE_CODES
    try:
      arguments = json.loads(arguments)
    except Exception as e:
      raise Exception(f"API arguments should be configured as a proper json. Error: {e}")
    try:
      request_type = arguments['request_type']
      url = arguments['url']
      payload = arguments.get('payload', {})
      ignore_failure = arguments.get('ignore_failure', False)
      timeout = arguments.get('timeout', 600)
      service_account_email = arguments.get('service_account_email', '')
      slack_client = sl.slack_notification(
          project_name=Variable.get('gcp_project'),
          slack_secret_name="slack_notification_token",
      )
    except KeyError as k:
      raise Exception(f"Mandatory argument {k} missing for task: '{task}'")

    if request_type not in ALLOWED_HTTP_REQUEST_TYPES:
      raise Exception(f"Invalid request type")

    # token is fetched using here
    try:
        token = idtoken_from_metadata_server(url, service_account_email=service_account_email)
    except Exception as e:
        message = {
            "text": f"""Attention <!channel>, Failed to authenticate url {url}. Error: {e}"""
        }
        slack_client.send_slack_message(message, thread_ts="")
        if not ignore_failure:
            raise Exception(f"Failed to authenticate url {url}. Error: {e}")
        else:
            return True
        
    
    if payload:
        payload = json.dumps(payload)
    headers = {
      'Authorization': token,
      'Content-Type': 'application/json'
    }

    response = requests.request(request_type, url, headers=headers, data=payload, timeout=timeout)
    if (not ignore_failure) and response.status_code not in ALLOWED_HTTP_RESPONSE_CODES:
        raise Exception(f"Trigger for '{task}' received error response: {response.content}")
    
    # Ignore failure but try to send a slack notification.
    elif ignore_failure and response.status_code not in ALLOWED_HTTP_RESPONSE_CODES:
        # Use a try except block just in case sending slack notification has some issues
        log.error(f"""Task {task} failed with Error: {response.content}""")
        
        # Try to send a notification about the error.
        message = {
            "text": f"""Attention <!channel>, {Variable.get('gcp_project')} - {Variable.get('pipeline')} Task {task} failed with Error: {response.content} :x:\nIgnoring error and continuing pipeline"""
        }
        slack_client.send_slack_message(message, thread_ts="")

    return True



    

