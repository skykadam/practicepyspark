from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
sc=spark.sparkContext
data="D:\\data engineering\\BigData\\datasets\\wcdata.txt"
rdd=sc.textFile(data)
stopword=["and","the","for"]
# Apply flatMap to split each line into words, then filter out stopwords
# Map each word to (word, 1), then reduceByKey to count occurrences
res = (rdd.flatMap(lambda x: x.split(" "))
           .filter(lambda x: len(x) > 2)
           .filter(lambda x: x.lower() not in stopword)
           .map(lambda x: (x, 1))
           .reduceByKey(lambda a, b: a + b)
           .sortBy(lambda x: x[1], ascending=False))

for element in res.collect():
    print(element)