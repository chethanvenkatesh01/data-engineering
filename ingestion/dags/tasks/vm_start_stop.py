from google.cloud import compute_v1
from airflow.models import Variable
from tasks import api_utils
from tasks.utils import idtoken_from_metadata_server
import requests

def call_vm_stop_api(instance_name):
    """
    Calls the VM stop API for the given instance.

    Args:
        instance_name (str): The name of the instance to stop.

    Returns:
        None
    """
    # Get the tenant and pipeline information from Airflow variables
    tenant = Variable.get("tenant")
    tenant = tenant.replace('_','-')
    pipeline = Variable.get("pipeline")
    instance_type = 'ingestion' if 'ingestion' in instance_name.split('-') else 'sourcing'
    # Construct the API URL using details from the environment and instance name
    url = f"{api_utils.HTTPS}{tenant}{api_utils.DEPLOY_ENV_MAPPING[pipeline].value}.{api_utils.DOMAIN_NAME}{api_utils.VM_STOP_URL}"
    
    # Extract project ID and determine the instance zone
    print('\npipeline_type',instance_type)
    project_id = get_project(instance_type)
    print("\nproject: ",project_id)
    zone = get_instance_zones(project_id, instance_name)
    
    print("\n*****Zone****\n", zone)
    
    # Obtain the identity token for authentication
    identity_token = idtoken_from_metadata_server(url)

    # Prepare the request payload
    payload = {
        "project_id": project_id,
        "zone": zone,
        "instance_name": instance_name
    }

    # Set up headers with the identity token for authentication
    headers = {
        "Authorization": f"Bearer {identity_token}",
        "Content-Type": "application/json"
    }

    print('\n***url***\n', url)
    print('\n***payload***\n', payload)

    # If the instance type is "ingestion", execute the stop API call
    expected_instance_type = ['ingestion','sourcing']
    if instance_type not in expected_instance_type:
        print(f"Unexpected instance type: {instance_type}. Expected: {expected_instance_type}")
        return

    if instance_type == 'ingestion':
        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code == 200:
                result = response.json()
                print("VM stop API called successfully:", result)
            else:
                error_message = response.text
                print(f"Error calling VM stop API: {response.status_code}, {error_message}")
        except requests.RequestException as e:
            print(f"Exception occurred while calling VM stop API: {e}")
    else:
        try:
            requests.post(url, json=payload, headers=headers)
        except requests.RequestException as e:
            print(f"Error making fire-and-forget request: {e}")

def get_project(pipeline_type):
    """
    Determines the project ID based on the pipeline_type environment.
    
    Returns:
        str: Project ID
    """
    if pipeline_type == 'sourcing':
        return Variable.get("reading_project")
    else:
        return Variable.get("gcp_project")

def get_instance_zones(project_id, instance_name):
    """
    Retrieves the zone for a given instance in a Google Cloud project.

    Args:
        project_id (str): The Google Cloud project ID.
        instance_name (str): The name of the instance whose zone is needed.

    Returns:
        str: The zone where the instance is located.
    """
    instance_client = compute_v1.InstancesClient()

    request = compute_v1.AggregatedListInstancesRequest(project=project_id,filter=f'name = "{instance_name}"')

    response = instance_client.aggregated_list(request=request)

    # Iterate through zones to find the instance.
    for zone, instances_scoped_list in response:
        if instances_scoped_list.instances:
            zone_name = zone.split('/')[-1]
            print(f"Found instance {instance_name} in zone: {zone_name}")
            return zone_name

    raise ValueError(f"Instance {instance_name} not found in project {project_id}.")