#!/bin/bash

# Set working directory
WORKDIR="/opt/airflow/dags/ingestion"  # Update this to your actual working directory
BITBUCKET_BRANCH="develop/dev"  # Update this to your actual branch name
BRANCH_ENVIRONMENT="dev"  # Update this to your actual environment

# Change to the working directory
cd $WORKDIR

# Set permissions for .env file
sudo chmod 777 .env

# Stash any local changes
sudo git stash

# Checkout the specified branch
sudo git checkout $BITBUCKET_BRANCH

# Bring down any running Docker containers
sudo docker compose down

# Pull the latest changes from the specified branch
sudo git pull origin $BITBUCKET_BRANCH

# Install jq if not already installed
sudo apt-get update
sudo apt-get install jq -y

# Add tenant environment variables
sudo ./add_tenant_env.sh $1 $BRANCH_ENVIRONMENT

# Build Docker images
sudo docker compose build

# Start the Airflow services with specified scaling
sudo docker compose up --scale airflow-worker=2 --scale airflow-scheduler=2 -d