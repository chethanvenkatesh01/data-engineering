#!/bin/sh
export PROJECT=data-ingestion-342714
export REPO=py-dataflow-pipelines
export TAG=latest
export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG

#gcloud builds submit . --tag $IMAGE_URI
docker build --no-cache -t $IMAGE_URI .

docker push $IMAGE_URI