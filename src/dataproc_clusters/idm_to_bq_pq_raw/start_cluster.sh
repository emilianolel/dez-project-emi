#!/bin/bash
set -e

#SERVICE_ACCOUNT_FILE_NAME=$GOOGLE_APPLICATION_CREDENTIALS
DATAPROC_CLUSTER=dez-cluster
DATAPROC_REGION=us-central1
GCP_PROJECT=dez-workspace-emil
CLUSTER_CONFIG=/opt/airflow/src/dataproc_clusters/idm_to_bq_pq_raw/request.json

echo
echo 'SETTING UP SERVICE ACCOUNT'
#gcloud auth activate-service-account --key-file=$SERVICE_ACCOUNT_FILE_NAME
gcloud config set project $GCP_PROJECT

# echo
# echo 'START CLUSTER'
# gcloud dataproc clusters start $DATAPROC_CLUSTER --region=$DATAPROC_REGION

echo
curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d @${CLUSTER_CONFIG} \
    "https://dataproc.googleapis.com/v1/projects/${GCP_PROJECT}/regions/${DATAPROC_REGION}/clusters"

gcloud dataproc clusters list --region=$DATAPROC_REGION
