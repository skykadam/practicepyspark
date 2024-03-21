from pyspark.sql import *
from pyspark.sql.functions import concat_ws ,col ,regexp_replace ,reverse, lit ,when,count,collect_list
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()

data="D:\\data engineering\\BigData\\datasets\\us-500.csv"

df=spark.read.format("csv").option("header","true").load(data)

#res=df.groupBy(col("state")).agg(count("*").alias("cnt"))
res=df.groupBy(col("state")).agg(count("first_name").alias("cnt"),collect_list("first_name").alias("first_name"))
#the collect_list function aggregates the fruit_name for each name into a list.
#collect_set using dont allowed duplicate value
res.show(truncate=False)
