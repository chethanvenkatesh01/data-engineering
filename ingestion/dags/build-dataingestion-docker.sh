#!/bin/sh
export PROJECT=impactsmart
export REPO=dataingestion-dataflow-pipelines
export TAG=latest
export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG

gcloud builds submit . --tag $IMAGE_URI
#docker build -t $IMAGE_URI .

#docker push $IMAGE_URI