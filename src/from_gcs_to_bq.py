#!/usr/bin/env python
# coding: utf-8

# In[3]:


SOURCE_FILE_PATH = 'gs://landing_bucket_dez/pq/mex_coords/*'
TARGET_TABLE = 'raw_geo_mx.mexico_coordinates'


# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.functions import udf

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ShortType, ByteType, DateType


# In[2]:


CREDENTIALS_LOCATION = '/home/emilel/.secrets/gcp/gcp-secret.json'
GCS_CONNECTOR_JAR = '/home/emilel/dez-project-emi/lib/gcs-connector-hadoop3-2.2.5.jar'
BQ_CONNECTOR_JAR = '/home/emilel/dez-project-emi/lib/spark-3.3-bigquery-0.36.1.jar'
JARS = f'{GCS_CONNECTOR_JAR},{BQ_CONNECTOR_JAR}'
DATAPROC_TEMP_BUCKET = 'dataproc-temp-us-central1-329749248489-tvazyaju'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set('spark.jars', JARS) \
    .set('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
    .set('spark.hadoop.google.cloud.auth.service.account.json.keyfile', CREDENTIALS_LOCATION)

sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel('WARN')
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set('fs.AbstractFileSystem.gs.impl',  'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS')
hadoop_conf.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
hadoop_conf.set('fs.gs.auth.service.account.json.keyfile', CREDENTIALS_LOCATION)
hadoop_conf.set('fs.gs.auth.service.account.enable', 'true')

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', DATAPROC_TEMP_BUCKET)


# In[7]:


df = spark.read.parquet(SOURCE_FILE_PATH)


# In[8]:


df.write.mode("overwrite").format("bigquery").option("table", TARGET_TABLE).save()


# In[ ]:




