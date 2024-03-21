from pyspark.sql import *

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
host = "jdbc:oracle:thin:@//oradbo.cdcwqh07mdjm.ap-south-1.rds.amazonaws.com:1521/ORCL"
df = spark.read \
    .format("jdbc") \
    .option("url", host) \
    .option("user", "ouser") \
    .option("password", "opassword") \
    .option("dbtable", "EMP") \
    .option("driver", "oracle.jdbc.OracleDriver") \
    .load()
df.show()

