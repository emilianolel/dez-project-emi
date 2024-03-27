#!/bin/bash
set -e

SERVICE_ACCOUNT_FILE_NAME=$GOOGLE_APPLICATION_CREDENTIALS
DATAPROC_CLUSTER=dez-cluster
DATAPROC_REGION=us-central1
GCP_PROJECT=dez-workspace-emil

echo
echo 'SETTING UP SERVICE ACCOUNT'
gcloud auth activate-service-account --key-file=$SERVICE_ACCOUNT_FILE_NAME
gcloud config set project $GCP_PROJECT

echo
echo 'CHECKING CLUSTER STATUS'
status=$(gcloud dataproc clusters list --region=$DATAPROC_REGION | grep $DATAPROC_CLUSTER | awk -F ' ' '{print $4}')

echo
if [[ $status == 'STOPPED' ]]; then
    echo
    echo 'STARTING CLUSTER'
    gcloud dataproc clusters start $DATAPROC_CLUSTER --region=$DATAPROC_REGION
    exit 0
fi

echo
echo 'CLUSTER ALREADY STARTED'
