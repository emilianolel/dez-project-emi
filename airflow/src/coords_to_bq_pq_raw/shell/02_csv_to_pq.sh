#!/bin/bash
set -e

SERVICE_ACCOUNT_FILE_NAME=$GOOGLE_APPLICATION_CREDENTIALS
PYTHON_DIR=/opt/airflow/src/coords_to_bq_pq_raw/pyspark/
PYTHON_FILE=formating_coords_csv.py
DATAPROC_CLUSTER=dez-cluster
DATAPROC_REGION=us-central1
GCP_PROJECT=dez-workspace-emil
SOURCE_FILE_PATH='gs://landing_bucket_dez/mex_coords/mex_coords.csv'
TARGET_FILE_PATH='gs://parquet_bucket_dez/pq/mex_coords/'

echo
echo 'SETTING UP SERVICE ACCOUNT'
gcloud auth activate-service-account --key-file=$SERVICE_ACCOUNT_FILE_NAME
gcloud config set project $GCP_PROJECT

echo
echo 'STARTING FROM CSV TO PARQUET'

gcloud dataproc jobs submit pyspark $PYTHON_DIR$PYTHON_FILE --cluster=$DATAPROC_CLUSTER --region=$DATAPROC_REGION -- --source_file=$SOURCE_FILE_PATH --target_file=$TARGET_FILE_PATH

echo
echo 'DONE!'
