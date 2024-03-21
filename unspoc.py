from pyspark.sql import *
from pyspark.sql import functions as F
from test import sc

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
sc=spark.sparkContext

data="D:\\data engineering\\BigData\\datasets\\un.txt"
ardd=sc.textFile(data)

fst=ardd.first()

res=ardd.flatMap(lambda x:x.split(" ")).map(lambda  x: (x,1)).reduceByKey(lambda x ,y: x+y)

for i in res.collect():
    print(i)

