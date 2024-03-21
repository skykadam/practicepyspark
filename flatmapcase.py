from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
sc=spark.sparkContext
#lineslist = ["Hello", "How are you doing", "Let us perform word count", "As part of the word count program", "we will see how many times each word repeat"]
#lines= sc.parallelize(lineslist)
#words = lines.flatMap(lambda l: l.split(" "))
#tuples=words.map(lambda word:(word,1))
#for i in tuples.countByKey():
 #   print(i)
data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)

for element in rdd.collect():
    print(element)

#Flatmap
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

