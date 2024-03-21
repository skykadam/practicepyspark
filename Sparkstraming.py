from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
#Create a DStream that will connect to hostname:port, like localhost:9999
# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 11)
# in office most recomnadation
#ssc=StreamingContext(spark.sparkContext,11)
lines = ssc.socketTextStream("ec2-13-201-133-224.ap-south-1.compute.amazonaws.com", 1236)
lines.pprint()
# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

...

# DataFrame operations inside your streaming program

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        cols=['name','age','city']
        df1 = rdd.map(lambda w: w.split(",")).toDF(cols)

        #wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        df1.createOrReplaceTempView("tab")

        # Do word count on table using SQL and print it
        res= spark.sql("select * from tab")
        res.show()
        # Write the resulting DataFrame into the Cassandra table
        res.write.mode("append").format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "akashdb") \
            .option("table", "asltb") \
            .save()
        # if you want save table in cassandra then create one table in advanced

    except:
        pass

res=lines.foreachRDD(process)

ssc.start()             ## Start the computation
ssc.awaitTermination()  ## Wait for the computation to terminate
#rdd...sc ...sparkcontext
## datafeame... sqlcontext
##dstream.... sparkcontext/streamingContexta