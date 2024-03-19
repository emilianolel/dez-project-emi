# Use the official Airflow image as the base
FROM apache/airflow:latest

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/usr/local/airflow

# Switch to the root user
USER root

# Create the AIRFLOW_HOME directory and change its ownership to the airflow user
RUN mkdir -p ${AIRFLOW_HOME} && chown -R airflow: ${AIRFLOW_HOME}

# Downloading gcloud package
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

# Adding the package path to local
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

# GCP creds
RUN mkdir -p /tmp/keys

COPY .secrets/gcp/gcp-secret.json /tmp/keys/gcp-secret.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/gcp-secret.json

RUN gcloud auth activate-service-account dez-project-emil@dez-workspace-emil.iam.gserviceaccount.com --key-file=/tmp/keys/gcp-secret.json --project=dez-workspace-emil

# Switch back to the airflow user
USER airflow

# Initialize the Airflow database
RUN airflow db init

# Create cfg file
RUN touch ${AIRFLOW_HOME}/airflow.cfg

# Customize the airflow.cfg file
RUN echo "[core]" > ${AIRFLOW_HOME}/airflow.cfg && \
    echo "airflow_home = ${AIRFLOW_HOME}" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "executor = LocalExecutor" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "[webserver]" > ${AIRFLOW_HOME}/airflow.cfg && \
    echo "base_url = http://localhost:8080" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "web_server_host = 0.0.0.0" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "web_server_port = 8080" >> ${AIRFLOW_HOME}/airflow.cfg

# Install python packages
WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt

