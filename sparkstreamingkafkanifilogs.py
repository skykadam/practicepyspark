from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
 

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()

ssc = StreamingContext(spark.sparkContext, 31)

# Define Kafka parameters

# Define the topics to subscribe to
topics = "nifi" # Use a list
kafkaparams = {"metadata.broker.list": "localhost:2181","auto.offset.reset":"smallest"}
# Create a Kafka direct stream
kvs= KafkaUtils.createDirectStream(ssc,"localhost:2181","spark-streaming-consumer", {topics:1})
lines=kvs.map(lambda x:x[1])

# Print the received messages from Kafka
lines.pprint()
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        # Convert RDD[String] to DataFrame if it's JSON-formatted
        df = spark.read.json(rdd)
        df.createOrReplaceTempView("tab")
        res = spark.sql("SELECT * FROM tab")
        res.show()
    except Exception as e:
        print("Error occurred: ", str(e))

res=lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
