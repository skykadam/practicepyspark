from pyspark.sql import *
from pyspark.sql.functions import concat_ws ,col ,regexp_replace ,reverse, lit ,when,count
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()

data="D:\\data engineering\\BigData\\datasets\\us-500.csv"

df=spark.read.format("csv").option("header","true").load(data)

res=df.groupBy(col("state")).agg(count("*").alias("cnt"))
#df.show()
 

#res = spark.sql("select *, concat_ws('_', first_name, last_name, state) as fullname from tab")
#res = spark.sql("select concat(first_name, ' ', last_name, ' ', state) as fullname, first_name, last_name, state, city, address, email from tab")
#res.show()
#res = df.withColumn('fullname', concat_ws(" ", col("first_name"), col("last_name"), col("state")))
#res = df.select(col("first_name"), col("last_name"), col("state"), concat_ws(" ", col("first_name"), col("last_name"), col("state")).alias("fullname"))

# to replace the substring "gmail" with "googlegmail" in the "email" column of the DataFrame.

#res=df.withColumn("regrep",regexp_replace(col("email"),"gmail","googlegmail")) \
    #.select(col("regrep"),col("first_name"),col("last_name"),col("state"),concat_ws(" ",col("first_name"),col("last_name"),col("state")).alias ("fullnmae"))
#res.show(truncate=False)

#reverse functon use
#res=df.withColumn("regrep",regexp_replace(col("email"),"gmail","googlegmail")) \
    #.select(col("regrep"),reverse (col("first_name")).alias ("first_name"),col("last_name"),col("state"),concat_ws(" ",col("first_name"),col("last_name"),col("state")).alias ("fullnmae"))
#res.show(truncate=False)

# withcolumn use in 2 use cases 1] add one new column if column name doesnot exist,2] update column  if column already exist.

#res = df.withColumn("age", lit(18)) #lit  create dummy values we can use any values like lit("testing")
#when condition use with withcolumn function
#res=df.withColumn("age",lit(18)).withColumn("state",when(col("state")=="OH","OHIO").when(col("state")=="CA","cali").otherwise(col("state")))
res.show(16,truncate=False)


