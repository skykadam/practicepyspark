from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
data="D:\\data engineering\\BigData\\datasets\\customers.csv "
df=spark.read.format("csv").options(header="true").load(data)
df.show()