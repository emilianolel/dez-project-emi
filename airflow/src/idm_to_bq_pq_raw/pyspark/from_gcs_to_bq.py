#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession


DATAPROC_TEMP_BUCKET = 'dataproc-staging-us-central1-329749248489-jq1oe9c7'
PARQUET_FILE = 'gs://parquet_bucket_dez/pq/idm/*'

spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', DATAPROC_TEMP_BUCKET)


print('READING PARQUET FILE')

idm_df = spark.read\
    .option("encoding", "ISO-8859-1")\
    .parquet('gs://landing_bucket_dez/pq/idm/*')\
    .na.drop(subset=['info_month_date'])


print('WRITING TABLE IN BQ')

idm_df.write\
    .mode("overwrite")\
    .format("bigquery")\
    .option('partitionField', 'info_month_date')\
    .option('clusteredFields', 'entity_code')\
    .option("encoding", "ISO-8859-1")\
    .option("table", "raw_dez_crimes.raw_municipal_crime_incidence")\
    .save()

print('DONE!')
