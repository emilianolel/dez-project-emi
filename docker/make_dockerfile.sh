#!/bin/bash


# Read .env files and overwites Dockerfile

cat airflow/airflow.env | awk '{if (length($1)) print "ARG " $1}' > Dockerfile

head -n 3 docker/Dockerfile.bak >> Dockerfile

cat airflow/airflow.env | awk -F "=" '{if (length($1)) print "ARG " $1}' >> Dockerfile

tail -n +3 docker/Dockerfile.bak >> Dockerfile
