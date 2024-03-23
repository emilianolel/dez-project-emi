#!/usr/bin/env python
# coding: utf-8


SOURCE_FILE_PATH = 'gs://landing_bucket_dez/mex_coords/mex_coords.csv'
TARGET_FILE_PATH = 'gs://landing_bucket_dez/pq/mex_coords/'


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from argparse import ArgumentParser
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()


parser = ArgumentParser(
    prog='from geo mex to Parquet',
    description='Program that reads geo information and writes as parquet in gcs'
)

parser.add_argument('--source_file')
parser.add_argument('--target_file')

args = parser.parse_args()


coords_schema = StructType([
    StructField('entity_code', StringType(), True)
    , StructField('entity_name', StringType(), True)
    , StructField('entity_name_short', StringType(), True)
    , StructField('municipality_code', StringType(), True)
    , StructField('municipality_name', StringType(), True)
    , StructField('location_code', StringType(), True)
    , StructField('location_name', StringType(), True)
    , StructField('scope_code', StringType(), True)
    , StructField('latitude', FloatType(), True)
    , StructField('longitude', FloatType(), True)
    , StructField('altitude', IntegerType(), True)
    , StructField('letter_key', StringType(), True)
])


coords_df = spark.read\
    .option('header', True)\
    .schema(coords_schema)\
    .csv(args.source_file)


coords_df = coords_df.withColumn('entity_name_short', F.regexp_replace('entity_name_short', '\.', '')) \
    .withColumn('entity', F.col('entity_name_short'))


coords_df.write\
    .mode("overwrite")\
    .partitionBy('entity')\
    .parquet(args.target_file)
