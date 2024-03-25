ARG AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-metadata/airflow
ARG AIRFLOW__CORE__EXECUTOR=LocalExecutor
ARG AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
ARG AIRFLOW__CORE__LOAD_EXAMPLES=False
ARG AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth
ARG APACHE_AIRFLOW_VERSION=latest
ARG AIRFLOW_HOME=/opt/airflow
ARG GCLOUD_PACKAGE_LINK=https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz
ARG GOOGLE_TEMP_DIR=/tmp/google-cloud-sdk.tar.gz
ARG USR_GCLOUD_DIR=/usr/local/gcloud
ARG GCLOUD_SDK_TAR=/tmp/google-cloud-sdk.tar.gz
ARG GCLOUD_SDK_INSTALL=/usr/local/gcloud/google-cloud-sdk/install.sh
ARG GCLOUD_BIN=/usr/local/gcloud/google-cloud-sdk/bin
ARG USER_GCP_SECRET_PATH=.secrets/gcp/
ARG USER_GCP_SECRET_NAME=gcp-secret.json
ARG CONTAINER_GCP_SECRET_FILE=/tmp/keys/gcp-secret.json
ARG GCP_SERVICE_ACCOUNT=dez-project-emil@dez-workspace-emil.iam.gserviceaccount.com
ARG GCP_PROJECT=dez-workspace-emil
ARG WORKDIR=/airflow
ARG REQUIREMENTS_FILE=requirements.txt
ARG ENTRYPOINT=entrypoint.sh
ARG DBT_DIR=/opt/dbt/mx_crimes
# Use the official Airflow image as the base
FROM apache/airflow:${APACHE_AIRFLOW_VERSION}

ARG AIRFLOW__CORE__SQL_ALCHEMY_CONN
ARG AIRFLOW__CORE__EXECUTOR
ARG AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION
ARG AIRFLOW__CORE__LOAD_EXAMPLES
ARG AIRFLOW__API__AUTH_BACKEND
ARG APACHE_AIRFLOW_VERSION
ARG AIRFLOW_HOME
ARG GCLOUD_PACKAGE_LINK
ARG GOOGLE_TEMP_DIR
ARG USR_GCLOUD_DIR
ARG GCLOUD_SDK_TAR
ARG GCLOUD_SDK_INSTALL
ARG GCLOUD_BIN
ARG USER_GCP_SECRET_PATH
ARG USER_GCP_SECRET_NAME
ARG CONTAINER_GCP_SECRET_FILE
ARG GCP_SERVICE_ACCOUNT
ARG GCP_PROJECT
ARG WORKDIR
ARG REQUIREMENTS_FILE
ARG ENTRYPOINT
ARG DBT_DIR

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Switch to the root user
USER root

# Create the AIRFLOW_HOME directory and change its ownership to the airflow user
RUN mkdir -p ${AIRFLOW_HOME} && chown -R airflow: ${AIRFLOW_HOME}

# Downloading gcloud package
RUN curl ${GCLOUD_PACKAGE_LINK} > ${GOOGLE_TEMP_DIR}

# Installing the package
RUN mkdir -p ${USR_GCLOUD_DIR} \
  && tar -C ${USR_GCLOUD_DIR} -xvf ${GCLOUD_SDK_TAR} \
  && ${GCLOUD_SDK_INSTALL}

# Adding the package path to local
ENV PATH $PATH:${GCLOUD_BIN}

# GCP creds
RUN mkdir -p /tmp/keys

COPY ${USER_GCP_SECRET_PATH}${USER_GCP_SECRET_NAME} ${CONTAINER_GCP_SECRET_FILE}

ENV GOOGLE_APPLICATION_CREDENTIALS=${CONTAINER_GCP_SECRET_FILE}

RUN gcloud auth activate-service-account ${GCP_SERVICE_ACCOUNT} --key-file=${GOOGLE_APPLICATION_CREDENTIALS} --project=${GCP_PROJECT}

USER ${AIRFLOW_UID:-50000}:0

# Install python packages
WORKDIR ${WORKDIR}

COPY ${REQUIREMENTS_FILE} ./

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r ${REQUIREMENTS_FILE}

