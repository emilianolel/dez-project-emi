#!/bin/bash
set -e

echo
echo 'CHANGE TO DBT DIRECTORY ${DBT_DIR}'
cd ${DBT_DIR}

echo
echo 'BUILDING MODELS'
dbt build --vars '{'is_test_run' : 'false'}'
