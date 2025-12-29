#!/bin/bash -e

# validate arguments
if [ $# -eq 0 ];
then
  echo "$0: Missing arguments"
  exit 1
elif [ $# -gt 2 ];
then
  echo "$0: Too many arguments: $@"
  exit 1
fi

# set variables
tenant=$1
module=$2
env=$(python3 ./fetch_branch_env.py --branch=$BITBUCKET_BRANCH)
project_name=$(python3 ./tenant_to_project_mapping.py --tenant_name=$tenant)


echo "Running GKE deployment for $tenant $module..."

# create image and upload it to image repo

echo "Image building: us-central1-docker.pkg.dev/$project_name/de-$module/de-$module:$env ..."
sudo docker build --build-arg module=$module -t us-central1-docker.pkg.dev/$project_name/de-$module/de-$module:$env .
sudo docker push us-central1-docker.pkg.dev/$project_name/de-$module/de-$module:$env


# set project
sudo gcloud config set project $project_name


# set cluster
# sudo gcloud container clusters get-credentials $tenant-generic --region us-central1 --project $project_name


# # # create namespace
# error=$(sudo kubectl get namespace $tenant-$module-$env || echo "failed")
# if [ "$error" = "failed" ]; then
#   echo "creating namespace $tenant-$module-$env..."
#   sudo kubectl create namespace $tenant-$module-$env 
# else
#   echo "namespace $tenant-$module-$env already exists."
# fi

# # # create service account
# error=$(sudo kubectl get serviceaccount $tenant-$module-$env -n $tenant-$module-$env || echo "failed")
# if [ "$error" = "failed" ]; then
#     echo "creating serviceaccount $tenant-$module-$env..."
#     sudo kubectl create serviceaccount $tenant-$module-$env -n $tenant-$module-$env
#     sudo gcloud iam service-accounts add-iam-policy-binding $tenant-$module-$env@$project_name.iam.gserviceaccount.com \
#       --role roles/iam.workloadIdentityUser \
#       --member "serviceAccount:$project_name.svc.id.goog[$tenant-$module-$env/$tenant-$module-$env]"
#     sudo kubectl annotate serviceaccount $tenant-$module-$env \
#     --namespace $tenant-$module-$env \
#     iam.gke.io/gcp-service-account=$tenant-$module-$env@$project_name.iam.gserviceaccount.com

# else
#   echo "Service account $tenant-$module-$env already exists."
# fi

# # upgrade airflow

# sudo helm repo add apache-airflow https://airflow.apache.org

# sudo helm repo update

# sudo helm upgrade --install $tenant-$module-$env apache-airflow/airflow \
#     --namespace $tenant-$module-$env\
#     --create-namespace \
#     -f deployment/chart.yaml \
#     --timeout=30m0s \
#     --debug \
#     --set images.airflow.repository=us-central1-docker.pkg.dev/$project_name/de-$module/de-$module \
#     --set images.airflow.tag=$env \
#     --set webserver.serviceAccount.name=$tenant-$module-$env \
#     --set scheduler.serviceAccount.name=$tenant-$module-$env \
#     --set workers.serviceAccount.name=$tenant-$module-$env \

sudo kubectl rollout restart deployment \
     $tenant-$module-$env-scheduler \
     $tenant-$module-$env-triggerer \
     $tenant-$module-$env-webserver \
     $tenant-$module-$env-worker \
     -n $tenant-$module-$env

sudo kubectl patch statefulset $tenant-$module-$env-postgresql \
    -n $tenant-$module-$env \
    -p '{"spec":{"template":{"spec":{"containers":[{"name":"postgresql","resources":{"requests":{"cpu":"2","memory":"4Gi"}}}]}}}}'

# Set environment variables dynamically
sudo kubectl set env deployment/$tenant-$module-$env-scheduler \
     GCP_PROJECT=$project \
     tenant=$tenant \
     pipeline=$env \
     -n $tenant-$module-$env

sudo kubectl set env deployment/$tenant-$module-$env-triggerer \
     GCP_PROJECT=$project \
     tenant=$tenant \
     pipeline=$env \
     -n $tenant-$module-$env

sudo kubectl set env deployment/$tenant-$module-$env-webserver \
     GCP_PROJECT=$project \
     tenant=$tenant \
     pipeline=$env \
     -n $tenant-$module-$env

sudo kubectl set env deployment/$tenant-$module-$env-worker \
     GCP_PROJECT=$project \
     tenant=$tenant \
     pipeline=$env \
     -n $tenant-$module-$env

sudo gcloud config set project ia-securearmor
