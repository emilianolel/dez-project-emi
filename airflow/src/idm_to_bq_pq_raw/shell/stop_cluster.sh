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
echo status=$(gcloud dataproc jobs list --cluster=$DATAPROC_CLUSTER --region=$DATAPROC_REGION --filter='status.state = ACTIVE' | grep JOB_ID | awk -F ' ' '{print $2}')
if [[ $status == '' ]]; then
    echo
    echo 'STOPPING CLUSTER'
    gcloud dataproc clusters stop $DATAPROC_CLUSTER --region=$DATAPROC_REGION
    exit 0
fi

echo
echo 'CLUSTER USED BY JOB ' $status
