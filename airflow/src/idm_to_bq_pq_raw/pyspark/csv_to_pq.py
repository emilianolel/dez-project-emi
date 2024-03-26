#!/usr/bin/env python
# coding: utf-8

LANDING_BUCKET = 'gs://landing_bucket_dez/'
PARQUET_FILE = 'pq/idm/'
CSV_FILE = 'idm.csv'

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ShortType, ByteType, DateType


spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

idm_schema = StructType([
    StructField('year', ShortType(), True)
    , StructField('entity_code', ByteType(), True)
    , StructField('entity_name', StringType(), True)
    , StructField('municipality_code', ShortType(), True)
    , StructField('municipality_name', StringType(), True)
    , StructField('affected_legal_asset', StringType(), True)
    , StructField('crime_type', StringType(), True)
    , StructField('crime_subtype', StringType(), True)
    , StructField('crime_modality_type', StringType(), True)
    , StructField('january', IntegerType(), True)
    , StructField('february', IntegerType(), True)
    , StructField('march', IntegerType(), True)
    , StructField('april', IntegerType(), True)
    , StructField('may', IntegerType(), True)
    , StructField('june', IntegerType(), True)
    , StructField('july', IntegerType(), True)
    , StructField('august', IntegerType(), True)
    , StructField('september', IntegerType(), True)
    , StructField('october', IntegerType(), True)
    , StructField('november', IntegerType(), True)
    , StructField('december', IntegerType(), True)
])


unpivoting_columns = ['year'
                      , 'entity_code'
                      , 'entity_name'
                      , 'municipality_code'
                      , 'municipality_name'
                      , 'affected_legal_asset'
                      , 'crime_type'
                      , 'crime_subtype'
                      , 'crime_modality_type']


stack_query_expression = '''
    STACK(12
    , "january", january
    , "february", february
    , "march", march
    , "april", april
    , "may", may
    , "june", june
    , "july", july
    , "august", august
    , "september", september
    , "october", october
    , "november", november
    , "december", december
    ) AS (month, crimes)
'''

month_dict = {
    'january'     : '01-01'
    , 'february'  : '02-01'
    , 'march'     : '03-01'
    , 'april'     : '04-01'
    , 'may'       : '05-01'
    , 'june'      : '06-01'
    , 'july'      : '07-01'
    , 'august'    : '08-01'
    , 'september' : '09-01'
    , 'october'   : '10-01'
    , 'november'  : '11-01'
    , 'december'  : '12-01'
}


@udf(returnType=StringType())
def get_first_day_of_month_date_udf(year, month_name):
    return str(year) + '-' + month_dict[month_name]


print('READING CSV FILE')
idm_df = spark.read\
    .option('header', True)\
    .schema(idm_schema)\
    .csv(LANDING_BUCKET + CSV_FILE)


print('TRANSFORMING DATA')
unpivoted_df = idm_df.selectExpr(*unpivoting_columns
                                 , stack_query_expression)


date_df = unpivoted_df.withColumn('info_month_date', get_first_day_of_month_date_udf('year', 'month').cast(DateType()))


date_df.cache


print('WRITING DATA')
date_df.write\
    .mode("overwrite")\
    .partitionBy('year')\
    .parquet(LANDING_BUCKET + CSV_FILE)

print('DONE!')
