#!/bin/bash

SERVICE_ACCOUNT_FILE_NAME=$GOOGLE_APPLICATION_CREDENTIALS
PYTHON_DIR=/opt/airflow/src/idm_to_bq_pq_raw/pyspark/
PYTHON_FILE=csv_to_pq.py
DATAPROC_CLUSTER=dez-cluster
DATAPROC_REGION=us-central1
GCP_PROJECT=dez-workspace-emil

echo
echo 'SETTING UP SERVICE ACCOUNT'
gcloud auth activate-service-account --key-file=$SERVICE_ACCOUNT_FILE_NAME
gcloud config set project $GCP_PROJECT

echo
echo 'STARTING FROM CSV TO PARQUET'

gcloud dataproc jobs submit pyspark $PYTHON_DIR$PYTHON_FILE --cluster=$DATAPROC_CLUSTER --region=$DATAPROC_REGION

echo
echo 'DONE!'
