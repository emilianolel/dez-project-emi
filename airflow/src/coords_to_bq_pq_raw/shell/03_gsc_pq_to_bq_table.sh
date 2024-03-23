#!/bin/bash
set -e

SERVICE_ACCOUNT_FILE_NAME=$GOOGLE_APPLICATION_CREDENTIALS
PYTHON_DIR=/opt/airflow/src/coords_to_bq_pq_raw/pyspark/
PYTHON_FILE=from_gcs_to_bq.py
DATAPROC_CLUSTER=dez-cluster
DATAPROC_REGION=us-central1
GCP_PROJECT=dez-workspace-emil
SOURCE_FILE_PATH='gs://landing_bucket_dez/pq/mex_coords/*'
TARGET_TABLE='raw_geo_mx.raw_mexico_coordinates'

echo
echo 'SETTING UP SERVICE ACCOUNT'
gcloud auth activate-service-account --key-file=$SERVICE_ACCOUNT_FILE_NAME
gcloud config set project $GCP_PROJECT

echo
echo 'STARTING PQ TO BQ TABLE'

gcloud dataproc jobs submit pyspark $PYTHON_DIR$PYTHON_FILE --cluster=$DATAPROC_CLUSTER --region=$DATAPROC_REGION -- --source_file=$SOURCE_FILE_PATH --target_table=$TARGET_TABLE

echo
echo 'DONE!'
