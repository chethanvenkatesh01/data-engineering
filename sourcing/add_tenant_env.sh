#!/bin/bash

# Check if arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 tenant pipeline"
    exit 1
fi

tenant="$1"
pipeline="$2"
secret_name="${tenant}_${pipeline}_data_ingestion"

# Check if .env file exists
if [ ! -f .env ]; then
    touch .env
fi

# Remove spaces around equal signs in .env file
sed -i 's/ *= */=/g' .env

# Function to fetch the tenant alias from secret
fetch_tenant_alias() {
    local tenant="$1"
    local pipeline="$2"
    local secret_name="${tenant}_${pipeline}_data_ingestion"
    # Fetch tenant_alias from Google Secret Manager
    # Assuming gcloud command is configured and authenticated
    tenant_alias_json=$(gcloud secrets versions access latest --secret="$secret_name" 2>/dev/null)
    if [[ $(echo "$tenant_alias_json" | jq '.["tenant-alias"]') != "null" ]] && [[ $(echo "$tenant_alias_json" | jq -r '.["tenant-alias"]') != "" ]]; then
        tenant_alias=$(echo "$tenant_alias_json" | jq -r '.["tenant-alias"]')
    else
        tenant_alias="$tenant"
    fi
    tenant_alias=$(echo "$tenant_alias" | sed 's/_/-/g')
    echo $tenant_alias
}

tenant_alias=$(fetch_tenant_alias "$tenant" "$pipeline")

# Function to check if variable exists in .env file
variable_exists() {
    local var_name="$1"
    while IFS='=' read -r name value; do
        if [ "$name" == "$var_name" ]; then
            return 0 # Variable exists
        fi
    done < .env
    return 1 # Variable does not exist
}

# Check if tenant variable is present in .env file
if ! variable_exists "tenant"; then
    echo "tenant='$tenant'" >> .env
fi

# Check if pipeline variable is present in .env file
if ! variable_exists "pipeline"; then
    echo "pipeline='$pipeline'" >> .env
fi

# Check if tenant_alias exists in .env file
# if it is not present, create one. If it is present, overwrite the existing value
if ! variable_exists "tenant_alias"; then
    echo "tenant_alias='$tenant_alias'" >> .env
else
    sed -i "s/^tenant_alias=.*/tenant_alias='$tenant_alias'/" .env
fi
