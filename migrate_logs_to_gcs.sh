#!/bin/bash -e

if [ $# -eq 0 ];
then
  echo "$0: Missing arguments"
  exit 1
elif [ $# -gt 2 ];
then
  echo "$0: Too many arguments: $@"
  exit 1
fi

if [ $2 == 'ingestion' ];
then
  WORKDIR=$INGESTION_WORKDIR
elif [ $2 == 'sourcing' ];
then
  WORKDIR=$SOURCING_WORKDIR
fi


vm_project=$(python3 ./fetch_vm_info.py --branch=$BITBUCKET_BRANCH --tenant=$1 --pipeline=$2)
hyphenated_tenant=$(echo $1 | sed 's/_/-/g')
INSTANCE_INFO=$(gcloud compute instances list --filter="name=$hyphenated_tenant-$2-$BRANCH_ENVIRONMENT" --format="get(networkInterfaces[0].networkIP, zone)" --project=$vm_project)

if [ -z "$INSTANCE_INFO" ]; then
  echo "No instance found with name: $hyphenated_tenant-$2-$BRANCH_ENVIRONMENT"
  exit 1
fi

# Extract the instance name and zone
vm_ip=$(echo $INSTANCE_INFO | awk '{print $1}')
INSTANCE_ZONE=$(echo $INSTANCE_INFO | awk '{print $2}')
echo "VM address: $vm_ip"

BRANCH_ENVIRONMENT=$(python3 ./fetch_branch_env.py --branch=$BITBUCKET_BRANCH)

hyphenated_tenant_name=$(echo "$1" | sed 's/_/-/g')

set -e

COMMANDS=(
    "cd $WORKDIR;"
    "sudo gsutil -m cp -r ./logs/* gs://$hyphenated_tenant_name-$2-$BRANCH_ENVIRONMENT/logs/;"
    "sudo rm -rf ./logs/;"
)

ssh -o StrictHostKeyChecking=no devops_impactanalytics_co@$vm_ip "${COMMANDS[@]}"
