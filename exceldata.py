from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import re

# Create Spark session
spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()

# Define the date format expected in your dataset
#date_format = "MM/dd/yy"

# Read the data and specify the date format
data = "D:\\data engineering\\BigData\\datasets\\sampledata.xlsx"
df = spark.read.format("com.crealytics.spark.excel") \
    .option("dataAddress", "'SalesOrders'!A1") \
    .option("header", "true") \
    .load(data)

# Round the TotalPrice column
cols = [re.sub('[^a-zA-Z0-9]', '', c).lower() for c in df.columns]
res = df.withColumn("Total", F.round(F.col("Total"))) \
    .toDF(*cols)

# Save data to a Cassandra table
res.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "akashdb") \
    .option("table", "excelsale") \
    .mode("append") \
    .save()

res.show()