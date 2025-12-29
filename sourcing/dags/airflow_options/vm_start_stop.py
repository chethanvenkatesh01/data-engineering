from google.cloud import compute_v1
from airflow.models import Variable
import requests
import time


def get_project():
    combined_tenant_list = ['rl-na','signet','vb','rl-eu','rl-eu-is','biglots']
    tenant = Variable.get("tenant_alias")
    pipeline = Variable.get("pipeline")

    if tenant in combined_tenant_list and pipeline in ['dev','test']:
        return 'impactsmart'
    elif tenant in combined_tenant_list and pipeline in ['uat','prod']:
        return 'impactsmart-prod'
    return Variable.get("gcp_project")

def start_vm_instance():
    """
    Starts a VM instance in Google Cloud Compute Engine using default credentials.

    Returns:
        None
    """
    # Get values from the Airflow variables
    project_id = get_project()
    tenant = Variable.get("tenant_alias")
    pipeline = Variable.get("pipeline")
    # Construct the instance name and get the zone
    instance_name = f"{tenant}-ingestion-{pipeline}"
    zone = get_instance_zones(project_id, instance_name)
    print("Project_id:", project_id)
    print("Zone:", zone)
    
    # Create the instance client and start the VM
    instance_client = compute_v1.InstancesClient()
    print(f"Starting instance {instance_name} in zone {zone}...")
    
    operation = instance_client.start(
        project=project_id,
        zone=zone,
        instance=instance_name
    )
    
    # Wait for the operation to complete
    wait_for_operation(operation, project_id, zone)

    # Run health check to check if the DAG is up
    run_health_check('ingestion')

    print(f"Instance {instance_name} started successfully.")


def wait_for_operation(operation, project_id, zone, timeout=300):
    """
    Waits for the operation to complete with a timeout.

    Args:
        operation: The operation object returned from an API call.
        project_id (str): Google Cloud project ID.
        zone (str): Zone where the operation is being performed.
        timeout (int): Maximum time to wait for the operation in seconds (default: 300s = 5 minutes).
    """
    operation_client = compute_v1.ZoneOperationsClient()
    start_time = time.time()

    while time.time() - start_time < timeout:
        result = operation_client.get(
            project=project_id,
            zone=zone,
            operation=operation.name
        )
        if result.status == compute_v1.Operation.Status.DONE:
            if result.error:
                raise Exception(f"Error during operation: {result.error}")
            print("Operation completed successfully.")
            return
        else:
            print("Operation in progress...")

        # Sleep for a short time to avoid spamming the API
        time.sleep(10)

    # If the timeout is reached, raise an exception or return an appropriate message.
    raise TimeoutError(f"Operation did not complete within {timeout} seconds.")

def fetch_vm_ip():
    """
    Fetches the VM IP using the fetch_vm_info.py script.

    Returns:
        str: The VM IP address, or None if an error occurs.
    """
    from urllib.parse import urlparse
    
    url = Variable.get('data_ingestion_pipeline_url')
    parsed_url = urlparse(url)
    return parsed_url.hostname

def health_check(vm_ip, dag_name, timeout=300, interval=5):
    """
    Performs a health check on the VM's DAG endpoint.

    Args:
        vm_ip (str): The IP address of the VM.
        dag_name (str): The name of the DAG to check.
        timeout (int): The maximum time to wait for a successful health check (in seconds).
        interval (int): The interval between health check retries (in seconds).

    Returns:
        bool: True if the health check is successful, False otherwise.
        int: The final HTTP response code from the DAG health check.
    """
    import time
    import requests
    elapsed = 0
    response = 0


    while elapsed < timeout:
        try:
            # Only checking if the service is reachable without logging in
            response = requests.get(
                f'http://{vm_ip}:8080/api/v1/dags/{dag_name}/dagRuns'
            ).status_code

            # Treat any HTTP status code as a valid response
            if response in [200, 401, 403]:  # 200 OK, 401 Unauthorized, 403 Forbidden, etc.
                print("Health check successful!")
                return True, response
            else:
                print(f"Health check failed for {dag_name} on {vm_ip}: {response}. Retrying...")
            
            time.sleep(interval)
            elapsed += interval
        
        except requests.RequestException as e:
            print(f"Error during health check: {e}")
            time.sleep(interval)
            elapsed += interval

    return False, response

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


def run_health_check(pipeline):
    """
    Main function that runs the health check process.

    Args:
        tenant (str): The tenant name.
        pipeline (str): The pipeline name.

    Returns:
        dict: A dictionary containing the result status, DAG name, VM IP, and response code.
    """
    vm_ip = fetch_vm_ip()
    print("vm_ip:", vm_ip)
    if not vm_ip:
        return {"status": "error", "message": "Failed to fetch VM IP"}

    dag_name = "data_ingestion" if pipeline == "ingestion" else None
    if not dag_name:
        return {"status": "error", "message": f"Unknown pipeline: {pipeline}"}

    print(f"Running health check for {dag_name} on {vm_ip}")
    success, response_code = health_check(vm_ip, dag_name)

    if success:
        return {
            "status": "success",
            "message": f"Health check successful for {dag_name} on {vm_ip}",
            "vm_ip": vm_ip,
            "dag_name": dag_name,
            "response_code": response_code
        }
        
    return {
        "status": "failed",
        "message": f"Health check failed for {dag_name} on {vm_ip}",
        "vm_ip": vm_ip,
        "dag_name": dag_name,
        "response_code": response_code
    }