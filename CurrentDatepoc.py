from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import current_date,col,to_date,datediff,last_day ,next_day,month,date_add,date_format,quarter,current_timestamp\
    ,months_between,trunc,date_trunc,dayofweek,dayofyear,dayofmonth,weekofyear,unix_timestamp

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()

data="D:\\data engineering\\BigData\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").load(data)
#config("spark.sql.session.timeZone", "EST")
#default yyyy-MM-dd
#WITH SQL SPARK
df.createOrReplaceTempView("tab")
# to_date() function convert strin in to date format
# current_date() dispaly the current date
#datediff() function differnence between current date column to another date column
#res1=spark.sql("select name , amount ,next_day(current_date(),'mon') nexmon ,current_date() today, to_date(dt,'dd-MM-yyyy') dt,datediff(current_date() ,to_date(dt,'dd-MM-yyyy')) datefiff from tab ")
res=df.withColumn("today",current_date()).withColumn("dt",to_date(col("dt"),"dd-MM-yyyy")) \
    .withColumn("dtdiff",datediff(col("today"),col("dt"))).withColumn("lastdt",last_day(col("dt")))\
    .withColumn("lasttue",next_day(date_add(last_day(col("dt")),-7),"tue"))\
    .withColumn("monthbt",months_between("today","dt",False))\
    .withColumn("celil",F.ceil(col("monthbt")))\
    .withColumn("floor",F.floor(col("monthbt")))\
    .withColumn("round",F.round(col("monthbt").cast ("int")))\
    .withColumn("truncyr",trunc(col("dt"),"month"))\
    .withColumn("datetru",date_trunc("month",col("dt")))\
    .withColumn("nextday",next_day(col("today"),"mon"))\
    .withColumn("onlymonth",month(col("dt")))\
    .withColumn("adddate",date_add(col("dt"),50))\
    .withColumn("addtold",date_add(col("dt"),-50))\
    .withColumn("dateformate",date_format(col("dt"),"EEEE"))\
    .withColumn("Qname",quarter(col("dt")))\
    .withColumn("currenttimestamp",current_timestamp())\
    .withColumn("dayofweek",dayofweek(col("dt")))\
    .withColumn("dayofmonth",dayofmonth(col("dt")))\
    .withColumn("dayofyr",dayofyear(col("dt")))\
    .withColumn("weekofYear",weekofyear(col("dt")))\
    .withColumn("uxt",unix_timestamp())
    #.where(col("Qname")==4)\

## trunc meAbs year or month display
## filter(col("Qname")==4) is aslo use
## date_formate tell dyas name of the dateof selected column (("E-dd-LLL-yyyy') day, date,month ,year)
##date_add is tell us next 50 date or -50 days date
##month see only month number of selected column
## next_day is return current day of the next data but if you paticular day then mention day and we next date on that day
## last_day is return last working day or last date current month  eg. 23-4-1993 return 31-4-1993
#res.printSchema()
res.show(truncate=False)
#res1.show()


