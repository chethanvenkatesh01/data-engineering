#!/bin/bash

if [ $# -eq 0 ];
then
  echo "$0: Missing arguments"
  exit 1
elif [ $# -gt 2 ];
then
  echo "$0: Too many arguments: $@"
  exit 1
fi

vm_ip=$1

if [ $2 == 'ingestion' ];
then
  DAGNAME='data_ingestion'
elif [ $2 == 'sourcing' ];
then
  DAGNAME='sourcing_intermediate_queries'
fi

echo "Running health check for $DAGNAME on $vm_ip"

TIMEOUT=120
INTERVAL=5
ELAPSED=0
RESPONSE=0

until [ $ELAPSED -ge $TIMEOUT ] || [ $RESPONSE -eq 200 ]; do
    RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -u airflow:airflow http://$vm_ip:8080/api/v1/dags/$DAGNAME/dagRuns)
    if [ "$RESPONSE" != "200" ]; then
    echo "Health check failed for $DAGNAME on $vm_ip: $RESPONSE. Retrying!!"
    sleep $INTERVAL
    ELAPSED=$((ELAPSED+INTERVAL))
    else
    echo "Health check successful!"
    fi
done
if [ $RESPONSE -ne 200 ]; then exit 1; fi
