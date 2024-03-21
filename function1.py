from pyspark.sql import *
from pyspark.sql.functions import concat_ws ,col ,regexp_replace ,reverse, lit ,when,count,collect_list,avg,round,floor,ceil,sum
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()

data="D:\\data engineering\\BigData\\datasets\\donations.csv"

df=spark.read.format("csv").option("header","true").load(data)
#The agg function is then used to calculate the average and sum of the "amount" column.
res=df.groupBy(col("name")).agg(avg("amount").alias("avgdon"),sum("amount").alias("totdon")) \
     .withColumn("round",round("avgdon")) \
     .withColumn("ceil",ceil("avgdon")) \
     .withColumn("floor",floor("avgdon"))
#round 45.51 ...46 if 45.49 we will get 45
#ceil 45.99 ....46 if 45.40 we will get 46
#floor 45.45....45 if 45.99 we will gwt 45
res.show()