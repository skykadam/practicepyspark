from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
df1=spark.read \
  .format("org.apache.spark.sql.cassandra") \
  .option("keyspace", "akashdb") \
  .option("table", "asl") \
  .load()
#df.show()
df2=spark.read \
  .format("org.apache.spark.sql.cassandra") \
  .option("keyspace", "akashdb") \
  .option("table", "emp") \
  .load()
#res=df1.join(df2,df1.name==df2.first_name)
# Assume df1 and df2 are already defined

# Perform the join operation
res = df1.join(df2, df1.name == df2.first_name, "outer").drop("first_name").fillna(0)

# Show the resulting DataFrame
res.show()

# Write the resulting DataFrame into the Cassandra table
res.write.mode("append").format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "akashdb") \
    .option("table", "jointb") \
    .save()
# if you want save table in cassandra then create one table in advanced