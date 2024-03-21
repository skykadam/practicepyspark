from pyspark.sql import *
from pyspark.sql import functions as F

from pyspark.sql.functions import rank,col,dense_rank,row_number,percent_rank,ntile
from pyspark.sql import window

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()

data="D:\\data engineering\\BigData\\datasets\\otherdatasets\\bank.csv"
df=spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
#df.show()
df.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab where age =(select max(age) from tab)")
#wp=Window.partitionBy("job").orderBy(col("age").desc())
#wp=Window.partitionBy("job").orderBy(col("balance").desc())
#Window between category based column and num col
# Rank
#res= (df.withColumn("Rank",rank().over(wp)).where("Rank =2")\
    #.withColumn("dense",dense_rank().over(wp)))\
    #.withColumn("row",row_number().over(wp))
wp=Window.partitionBy("job").orderBy(col("balance").desc())

res= df.limit(100).withColumn("Rank",rank().over(wp))\
    .withColumn("dense",dense_rank().over(wp))\
    .withColumn("row",row_number().over(wp)).withColumn("percent_rank",percent_rank().over(wp))\
    .withColumn("ntile",ntile(3).over(wp))

#Window function: returns the ntile group id (from 1 to n inclusive) in an ordered window
#partition. For example, if n is 4, the first quarter of the rows will get value 1,
#the second quarter will get 2, the third quarter will get
#3, and the last quarter will get 4.for example month


res.show(500)
