#!/bin/bash
set -e

TEMP_DIRECTORY=../tmp/mex_coords/

DOWNLOAD_LINK='https://www.inegi.org.mx/contenidos/app/ageeml/catun_localidad.zip'

CSV_FILE_NAME=mex_coords.csv

ZIP_FILE_NAME=mex_coords.zip

CSV_DIR_NAME=mex_coords/

BUCKET_PATH=gs://landing_bucket_dez/

echo
echo ' EXECUTING FILE: ' $0

echo
echo 'CREATING TEMP DIR'
mkdir -p $TEMP_DIRECTORY

echo
echo 'DOWNLOADING COORDS ZIP FILE'
curl -o $TEMP_DIRECTORY$ZIP_FILE_NAME -LJ $DOWNLOAD_LINK

echo
echo 'EXTRACTING COORDS CSV FILE'
unzip $TEMP_DIRECTORY$ZIP_FILE_NAME -d $TEMP_DIRECTORY
mv ${TEMP_DIRECTORY}AGEEML*.csv $TEMP_DIRECTORY$CSV_FILE_NAME

echo
echo 'MOVING IDM CSV FILE TO GCS'
gsutil -m cp $TEMP_DIRECTORY$CSV_FILE_NAME $BUCKET_PATH$CSV_DIR_NAME$CSV_FILE_NAME

echo
echo 'DELETING TEMP DIRECTORY'
rm -rf $TEMP_DIRECTORY

echo
echo 'DONE!'
