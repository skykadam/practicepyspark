from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
data="D:\\data engineering\\BigData\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").load(data)
#df.show()

df.createTempView("tab") # its allows to sql queries on the top of the dataframes
#res=spark.sql("select * from tab where state='NY' and email like '%@gmail%'")

#these code is python friendly

res=df.where((df.state== "CA") & (df.email.like("%@gmail%")))
res.show(truncate=False) # display only 20 records by default and if any value is more than 20 char show....
