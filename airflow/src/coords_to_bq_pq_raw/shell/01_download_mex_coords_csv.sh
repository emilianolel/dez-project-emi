#!/bin/bash
set -e

TEMP_DIRECTORY=../tmp/mex_coords/

DOWNLOAD_LINK='https://raw.githubusercontent.com/eduardoarandah/coordenadas-estados-municipios-localidades-de-mexico-json/master/data.csv'

CSV_FILE_NAME=mex_coords.csv

CSV_DIR_NAME=mex_coords/

BUCKET_PATH=gs://landing_bucket_dez/

echo
echo ' EXECUTING FILE: ' $0

echo
echo 'CREATING TEMP DIR'
mkdir -p $TEMP_DIRECTORY

echo
echo 'DOWNLOADING IDM CSV FILE'
curl -o $TEMP_DIRECTORY$CSV_FILE_NAME -LJ $DOWNLOAD_LINK

echo
echo 'MOVING IDM CSV FILE TO GCS'
gsutil -m cp $TEMP_DIRECTORY$CSV_FILE_NAME $BUCKET_PATH$CSV_DIR_NAME$CSV_FILE_NAME

echo
echo 'DELETING TEMP DIRECTORY'
rm -rf $TEMP_DIRECTORY

echo
echo 'DONE!'
