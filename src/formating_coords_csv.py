#!/usr/bin/env python
# coding: utf-8

# In[29]:


SOURCE_FILE_PATH = 'gs://landing_bucket_dez/mex_coords/mex_coords.csv'
TARGET_FILE_PATH = 'gs://landing_bucket_dez/pq/mex_coords/'


# In[4]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.functions import udf

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ShortType, ByteType, DateType, FloatType


# In[5]:


CREDENTIALS_LOCATION = '/home/emilel/.secrets/gcp/gcp-secret.json'
SPARK_LIB = "/home/emilel/dez-project-emi/lib/gcs-connector-hadoop3-2.2.5.jar"

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", SPARK_LIB) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", CREDENTIALS_LOCATION)

sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("WARN")
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", CREDENTIALS_LOCATION)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()


# In[31]:


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


# In[33]:


coords_df = spark.read\
    .option('header', True)\
    .schema(coords_schema)\
    .csv(SOURCE_FILE_PATH)


# In[34]:


coords_df = coords_df.withColumn('entity_name_short', F.regexp_replace('entity_name_short', '\.', '')) \
    .withColumn('entity', F.col('entity_name_short'))


# In[35]:


coords_df.write\
    .mode("overwrite")\
    .partitionBy('entity')\
    .parquet(TARGET_FILE_PATH)


# In[ ]:




