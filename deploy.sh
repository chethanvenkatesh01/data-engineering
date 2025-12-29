#!/bin/bash -e

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
  echo "$0: Incorrect number of arguments"
  echo "Usage: $0 <tenant_name> <pipeline_type> [redeploy]"
  exit 1
fi

if [ $2 == 'ingestion' ];
then
  WORKDIR=$INGESTION_WORKDIR
elif [ $2 == 'sourcing' ];
then
  WORKDIR=$SOURCING_WORKDIR
fi

REDEPLOY=${3:-false}  # Default value for redeploy is false

BRANCH_ENVIRONMENT=$(python3 ./fetch_branch_env.py --branch=$BITBUCKET_BRANCH)

vm_project=$(python3 ./fetch_vm_info.py --branch=$BITBUCKET_BRANCH --tenant=$1 --pipeline=$2)
echo "Pipeline: $2"
echo "Tenant: $1"
echo "VM Project: $vm_project"
hyphenated_tenant=$(echo $1 | sed 's/_/-/g')
INSTANCE_NAME="$hyphenated_tenant-$2-$BRANCH_ENVIRONMENT"
INSTANCE_INFO=$(gcloud compute instances list --filter="name=$INSTANCE_NAME" --format="get(networkInterfaces[0].networkIP, zone)" --project=$vm_project)

if [ -z "$INSTANCE_INFO" ]; then
  echo "No instance found with name: $hyphenated_tenant-$2-$BRANCH_ENVIRONMENT"
  exit 1
fi

# Extract the instance name and zone
vm_ip=$(echo $INSTANCE_INFO | awk '{print $1}')
INSTANCE_ZONE=$(echo $INSTANCE_INFO | awk '{print $2}')

echo "Found instance: $INSTANCE_NAME in zone: $INSTANCE_ZONE"

echo "VM IP: $vm_ip"

check_ssh() {
    for i in {1..10}; do
        # Try to connect to SSH port (22)
        echo "Checking SSH on $vm_ip... Try $i out of 10"
        if nc -z -w10 $vm_ip 22; then
            echo "SSH is up on $vm_ip"
            return 0
        else
            echo "Exit code of nc: $?"
            echo "SSH is down on $vm_ip"
            sleep 10;
        fi
    done
    echo "SSH did not come up after 10 tries."
    return 1
}

# Start the instance
echo "Starting instance $INSTANCE_NAME in zone $INSTANCE_ZONE..."
gcloud compute instances start $INSTANCE_NAME --zone $INSTANCE_ZONE --project=$vm_project

if [ $REDEPLOY == 'true' ];
  then echo "Redeploying $INSTANCE_NAME"
  if [ $2 == 'ingestion' ];
  then
    COMMANDS=(
        "cd $WORKDIR;"
        "sudo chmod 777 .env;"
        "sudo git stash;"
        "sudo git checkout $BITBUCKET_BRANCH;"
        "sudo docker compose down;"
        "sudo git pull origin $BITBUCKET_BRANCH;"
        "sudo apt-get install jq -y;"
        "sudo ./add_tenant_env.sh $1 $BRANCH_ENVIRONMENT;"
        "sudo docker compose build;"
        "sudo docker compose up --scale airflow-worker=2 --scale airflow-scheduler=2 -d;"
    )
  elif [ $2 == 'sourcing' ];
  then
    EXIT_CODE=0
    COMMANDS=(
        "cd $WORKDIR;"
        "sudo chmod 777 .env;"
        "sudo git stash;"
        "sudo git checkout $BITBUCKET_BRANCH;"
        "sudo docker compose down;"
        "sudo git pull origin $BITBUCKET_BRANCH;"
        "sudo apt-get install jq -y;"
        "sudo ./add_tenant_env.sh $1 $BRANCH_ENVIRONMENT;"
        "sudo docker compose build;"
        "sudo docker compose run --rm airflow-webserver airflow db init || EXIT_CODE=$?;"
        "echo $EXIT_CODE;"
        "sudo docker exec sourcing-postgres-1 psql -U airflow -c 'DROP TABLE alembic_version;';"
        "sudo docker compose run --rm airflow-webserver airflow db upgrade;"
        "sudo docker compose up --scale airflow-worker=2 --scale airflow-scheduler=2 -d;"
    )
  fi
else
  echo 'Redeploy is set to false. Just pulling latest code.'
  COMMANDS=(
    "cd $WORKDIR;"
    "sudo git stash;"
    "sudo git checkout $BITBUCKET_BRANCH;"
    "sudo git pull origin $BITBUCKET_BRANCH;"
  )
fi

if [ $? -eq 0 ]; then
    echo "Instance $INSTANCE_NAME started successfully."
    
    # Now check if SSH is available within the timeout period
    check_ssh
    
    if [ $? -eq 0 ]; then
        echo "Proceeding with SSH commands..."
        ssh -o StrictHostKeyChecking=no devops_impactanalytics_co@$vm_ip "${COMMANDS[@]}"
    else
        echo "Failed to establish SSH connection after multiple attempts."
        exit 1
    fi
else
    echo "Failed to start instance $INSTANCE_NAME."
    exit 1
fi

./healthcheck.sh $vm_ip $2

if [ $? -eq 0 ]; then
    echo "Healthcheck passed. Triggering Airflow DAG: load_env_vars_$2"
    DAG_TRIGGER_COMMAND=(
      "sudo docker exec \$(sudo docker ps -q -f "ancestor=$2-airflow-webserver") airflow dags trigger load_env_vars_$2"
    )
    echo ${DAG_TRIGGER_COMMAND[@]}
    ssh -o StrictHostKeyChecking=no devops_impactanalytics_co@$vm_ip "${DAG_TRIGGER_COMMAND[@]}"
    if [ $? -eq 0 ]; then
        echo "DAG load_env_vars_sourcing triggered successfully."
    else
        echo "Failed to trigger DAG load_env_variables."
        exit 1
    fi
else
    echo "Healthcheck failed for $vm_ip. Aborting DAG trigger."
    exit 1
fi