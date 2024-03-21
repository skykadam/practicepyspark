from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lead,lead, when,isnull
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
data = "D:\\data engineering\\BigData\\datasets\\donations.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)

# Define the window specification
wp = Window.partitionBy("name").orderBy(col("amount").desc())

# Apply window function to calculate lead and difference
res = df.withColumn("lead", lead("amount", 1).over(wp)) \
    .withColumn("lead", when(col("lead").isNull() | (col('lead')<0), 0).otherwise(col("lead")))\
    .withColumn("result",when(col("lead")==0,0).otherwise(col("amount")))\
    .withColumn("diff",col("amount") - col("lead"))

# Show the resulting DataFrame
res.show()
