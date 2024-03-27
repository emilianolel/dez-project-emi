#!/bin/bash
set -e

SERVICE_ACCOUNT_FILE_NAME=$GOOGLE_APPLICATION_CREDENTIALS
DATAPROC_CLUSTER=dez-cluster
DATAPROC_REGION=us-central1
GCP_PROJECT=dez-workspace-emil
CLUSTER_CONFIG=../../dataproc_clusters/idm_to_bq_pq_raw/cluster_config.json

echo
echo 'SETTING UP SERVICE ACCOUNT'
gcloud auth activate-service-account --key-file=$SERVICE_ACCOUNT_FILE_NAME
gcloud config set project $GCP_PROJECT

echo
echo 'START CLUSTER'
gcloud dataproc clusters start $DATAPROC_CLUSTER --region=$DATAPROC_REGION
