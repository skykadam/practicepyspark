from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
sc=spark.sparkContext

data="D:\\data engineering\\BigData\\datasets\\asl.csv"
ardd=sc.textFile(data)
fst=ardd.first()
# i want those age is more than 20
#res=ardd.filter(lambda x:x !=fst).map(lambda x:x.split(",")).filter(lambda x:int(x[1])>20)

res=ardd.filter(lambda x:x !=fst).map(lambda x:x.split(",")[2]).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
for i in res.collect():
    print(i)

# Split the lines by comma and filter the records where age is more than 40
#res= ardd.filter(lambda x: int(x.split(",")[1]) > 40)

# Print the filtered records
#for i in res.collect():
    #print(i)

# in realtime scenario most of rdd is (unstructure) covert structure and run sql queries spark 1.5 ...

res=ardd.filter(lambda  x:x !=fst).map(lambda x:x.split(",")).toDF(["name","age","city"])

# toDF convert to dataframe but data must be structure

res.createOrReplaceTempView("tab")
#result=spark.sql("select * from tab where age>30")
#result=spark.sql("select city ,count(*) cnt from tab group by city")
result=res.groupby(res.city).count()
result.show()
