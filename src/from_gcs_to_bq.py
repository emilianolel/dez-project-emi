#!/usr/bin/env python
# coding: utf-8

SOURCE_FILE_PATH = 'gs://landing_bucket_dez/pq/mex_coords/*'
TARGET_TABLE = 'raw_geo_mx.mexico_coordinates'

DATAPROC_TEMP_BUCKET = 'dataproc-staging-us-central1-329749248489-jq1oe9c7'


from pyspark.sql import SparkSession
from argparse improt ArgumentParser



spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', DATAPROC_TEMP_BUCKET)


parser = ArgumentParser(
    prog='from gcs to BQ',
    description='Program that reads gcs file and writes a bq table'
)

parser.add_argument('--source-file')
parser.add_argument('--target-table')

args = parser.parse_args()

print('READING PARQUET FILE')

df = spark.read.parquet(SOURCE_FILE_PATH)


print('WRITING TABLE IN BQ')


df.write.mode("overwrite").format("bigquery").option("table", TARGET_TABLE).save()

print('DONE!')
