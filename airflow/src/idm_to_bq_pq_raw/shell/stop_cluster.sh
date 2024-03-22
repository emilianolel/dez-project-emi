#!/bin/bash

SERVICE_ACCOUNT_FILE_NAME=$GOOGLE_APPLICATION_CREDENTIALS
DATAPROC_CLUSTER=dez-cluster
DATAPROC_REGION=us-central1
GCP_PROJECT=dez-workspace-emil

echo
echo 'SETTING UP SERVICE ACCOUNT'
gcloud auth activate-service-account --key-file=$SERVICE_ACCOUNT_FILE_NAME
gcloud config set project $GCP_PROJECT

echo
echo 'STOP CLUSTER'
gcloud dataproc clusters stop $DATAPROC_CLUSTER --region=$DATAPROC_REGION
