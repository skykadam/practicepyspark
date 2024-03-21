from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
sc=spark.sparkContext
data="D:\\data engineering\\BigData\\datasets\\donations.csv"
rdd=sc.textFile(data)
skip=rdd.first()
# reduceByKey used to group the values
#select name ,sum(amt)from tab group by name
# reduceByKey
res=(rdd.filter(lambda  x: x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))\
     .reduceByKey(lambda x,y:x+y)).sortBy(lambda  x:-x[1]) # -x[1] decending and x[x] asending
for i in res.collect():
    print(i)


