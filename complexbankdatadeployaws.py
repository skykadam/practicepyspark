from pyspark.sql import *
from pyspark.sql import functions as F

import sys

import re
#if want save data  in hive then add enableHiveSupport
spark = SparkSession.builder.master("local[2]").appName("testing").enableHiveSupport().getOrCreate()
#get data from hdfs that time use
#data="hdfs://localhost:9000/datasets/10000Records.csv"
#data="file:///datasets/10000Records.csv"
#get data from s3 in production env
data="D:\\data engineering\\BigData\\datasets\\10000Records.csv"
data=sys.argv[1]
tab=sys.argv[2]
df=spark.read.format("csv").option("header","true").option("inferShema","true").load(data)
#df.createTempView("tab")
ndf=df.toDF(*(re.sub(r'[^a-zA-z]','',c)for c in df.columns))
#df.show(truncate=False)
ndf.show(5,truncate=False)
ndf.write.format("hive").saveAsTable(tab) # arugument second