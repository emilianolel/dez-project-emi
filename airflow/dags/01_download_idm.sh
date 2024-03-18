#!/bin/bash

TEMP_DIRECTORY=../temp/idm/

IDM_DOWNLOAD_LINK='https://www.gob.mx/sesnsp/acciones-y-programas/datos-abiertos-de-incidencia-delictiva?state=published'

ZIP_FILE_NAME=idm.zip

CSV_FILE_NAME=idm.csv

BUCKET_PATH=gs://landing_bucket_dez/


download_idm() {
	
	google_idm_id=$(curl -s  "$1" | \
		grep -Po "(?<=href=\")[^\"][^\"]*(?=\">Cifras de Incidencia Delictiva Municipal, 2015.*)"  | \
		sed 's| |%20|g' | grep -oP '(?<=/)[0-9a-zA-Z_-]{20,}(?=/)')
        
	echo "Google IDM id: $google_idm_id"

        gdown $google_idm_id -O $TEMP_DIRECTORY$CSV_FILE_NAME

}


echo
echo ' EXECUTING FILE: ' $0

echo
echo 'CREATING TEMP DIRECORY'
mkdir $TEMP_DIRECTORY

echo
echo 'DOWNLOADING IDM CSV FILE'
download_idm $IDM_DOWNLOAD_LINK

echo
echo 'MOVING IDM CSV FILE TO GCS'
gsutil -m cp $TEMP_DIRECTORY$CSV_FILE_NAME $BUCKET_PATH

echo
echo 'DELETING TEMP DIRECTORY'
rm -rf $TEMP_DIRECTORY

echo
echo 'DONE!'
