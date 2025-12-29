### VM Setup

#### Docker setup

    docker compose build;
    docker compose up --scale airflow-worker=2 --scale airflow-scheduler=2 -d;
    docker compose down;

#### Local Docker commands
    docker rm $(docker ps -aq)
    docker rmi $(docker images -aq)
    docker volume remove $(docker volume ls -q)

---

### cluster setup

#### Keda
    helm repo add kedacore https://kedacore.github.io/charts
    helm repo update
    helm upgrade --install keda kedacore/keda --namespace keda --create-namespace


---

### Sourcing Setup

#### Build the image
The image is built based on Docker file in the root directory. Both ingestion and sourcing are build using the same Dockerfile(parameters are passed using --build-arg=sourcing/ingestion)

    docker build --build-arg module=sourcing -t us-central1-docker.pkg.dev/<project_name>/de-sourcing/de-sourcing:no_dags --progress=plain .
    docker push us-central1-docker.pkg.dev/<project_name>/de-sourcing/de-sourcing:no_dags
<br>

    helm upgrade \
    --install sourcing-vs-test apache-airflow/airflow \ 
    --namespace sourcing-vs-test \ 
    --create-namespace \ 
    -f deployment/sourcing-test.yaml \ 
    --timeout=30m0s \ 
    --debug

---     

### Ingestion Setup

    docker build --build-arg module=ingestion -t us-central1-docker.pkg.dev/arhaus-401512/de-ingestion/de-ingestion:no_dags --progress=plain .
<br>

    docker push us-central1-docker.pkg.dev/arhaus-401512/de-ingestion/de-ingestion:no_dags

### service account binding

    kubectl create namespaces arhaus-ingestion-test
<br>

kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
    annotations:
        iam.gke.io/gcp-service-account: arhaus-ingestion-dev@arhaus-401512.iam.gserviceaccount.com
        name: arhaus-ingestion-dev
        namespace: arhaus-ingestion-dev
EOF
<br>

    gcloud iam service-accounts add-iam-policy-binding \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:arhaus-401512.svc.id.goog[arhaus-ingestion-test/arhaus-ingestion-test]" \
    arhaus-ingestion-test@arhaus-401512.iam.gserviceaccount.com \
    --project=arhaus-401512

#### helm installation

    helm upgrade --install tenant-module-env apache-airflow/airflow \
    --namespace tenant-module-env\
    --create-namespace \
    -f deployment/ingestion-test.yaml \
    --timeout=30m0s \
    --debug \
    --set images.airflow.repository=us-central1-docker.pkg.dev/{project-name}/de-ingestion/de-ingestion

#### patch the postgres statefulset
    kubectl patch statefulset arhaus-ingestion-test-postgresql -n arhaus-ingestion-test -p '{"spec":{"template":{"spec":{"containers":[{"name":"postgresql","resources":{"requests":{"cpu":"2","memory":"4Gi"}}}]}}}}'

