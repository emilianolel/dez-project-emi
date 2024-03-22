#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession


DATAPROC_TEMP_BUCKET = 'dataproc-staging-us-central1-329749248489-jq1oe9c7'
PARQUET_FILE = 'gs://landing_bucket_dez/pq/idm/*'

spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', DATAPROC_TEMP_BUCKET)


print('READING PARQUET FILE')

idm_df = spark.read.parquet('gs://landing_bucket_dez/pq/idm/*')


print('WRITING TABLE IN BQ')

idm_df.write.mode("overwrite").format("bigquery").option("table", "raw_dez_crimes.raw_municipal_crime_incidence").save()

print('DONE!')