from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
data="D:\\data engineering\\BigData\\datasets\\bank-full.csv"

df=spark.read.format("csv").option("header","true").option("sep",";").load(data)
df.show()
df.createTempView("tab")
#res=spark.sql("select * from tab where balance > 25000 and marital = 'single'")
res=df.where((df.marital=='single') & (df.balance > 25000)) # using python
res.show()