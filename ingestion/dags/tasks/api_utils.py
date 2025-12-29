from enum import Enum

# HTTP Methods
class HTTP_methods(Enum):
    POST = "POST"
    PUT = "PUT"
    GET = "GET"
    PATCH = "PATCH"

# Constants
VM_STOP_URL = "/api/v2/data-platform/vm_stop"
STORE_CLUSTERING_INFO_FROM_FMD = "/api/v2/data-platform/cluster/generate-query-from-fmd"
LSI_CLUSTER_SUBMIT_JOB = "/api/v2/data-platform/cluster/lsi-store-cluster/submit-job"
SISTER_STORE_MAPPING = "/api/v2/data-platform/cluster/sister-store-mapping"
LSI_CLUSTER = "/api/v2/data-platform/cluster/lsi-store-cluster"
REDIS_CACHE_INVALIDATION = "/api/v2/core/ingestion/event"
DOMAIN_NAME = "iaproducts.ai"
CORE_DOMAIN_NAME = "impactsmartsuite.com"
HTTPS = "https://"

# deploy_env mapping
class DEPLOY_ENV_MAPPING(Enum):
    dev = ".devs"
    test = ".test"
    uat = ".uat"
    prod = ""

class API_methods():
    def post(self, url, payload, header):
        import json
        import requests
        payload = json.dumps(payload)
        response = requests.post(url, data=payload, headers=header)
        return response.json()
    def get(self, url, payload, header):
        import requests
        response = requests.get(url, params=payload, headers=header)
        return response.json()
    def put(self, url, payload, header):
        import json
        import requests
        payload = json.dumps(payload)
        response = requests.put(url, data=payload, headers=header)
        return response.json()
    def patch(self, url, payload, header):
        import json
        import requests
        payload = json.dumps(payload)
        response = requests.patch(url, data=payload, headers=header)
        return response.json()
    def delete(self, url, payload, header):
        import requests
        response = requests.delete(url, params=payload, headers=header)
        return response.json()

def poll_on_task_ids(task_ids:list) -> True:
    from airflow import models
    import urllib
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    from pandas import read_sql
    conn_string = (
            "postgresql://"
            + models.Variable.get("PGXUSER")
            + ":"
            + urllib.parse.quote(models.Variable.get("PGXPASSWORD"))
            + "@"
            + models.Variable.get("PGXHOST")
            + ":"
            + models.Variable.get("PGXPORT")
            + "/"
            + models.Variable.get("PGXDATABASE")
        )
    db_engine = create_engine(conn_string, poolclass=NullPool)
    query = f"""
        select task_id, status 
        from {models.Variable.get('PGXSCHEMA')}.cloud_task_status cts
        where task_id in ('{("','").join(task_ids)}')
    """
    poll_result = read_sql(query, db_engine)
    poll_result = {
        poll_result['task_id'].values[ind] : poll_result['status'].values[ind] 
        for ind in range(len(poll_result))
    }
    return poll_result

def get_unique_id():
    import uuid
    return str(uuid.uuid4()).replace("-", "")

def call_http_endpoint(url:str, method:str, payload:dict, headers:dict) -> dict:
    api = API_methods()
    if method == HTTP_methods.POST.value:
        return api.post(url, payload, headers)
    elif method == HTTP_methods.GET.value:
        return api.get(url, payload, headers)
    elif method == HTTP_methods.PUT.value:
        return api.put(url, payload, headers)
    elif method == HTTP_methods.PATCH.value:
        return api.patch(url, payload, headers)
    elif method == HTTP_methods.DELETE.value:
        return api.delete(url, payload, headers)
    else:
        raise Exception("Invalid HTTP method")