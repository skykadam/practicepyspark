from pyspark.sql import *
from pyspark.sql import functions as F

from pyspark.sql.functions import col ,explode_outer


from pyspark.sql.types import *

spark = SparkSession.builder.master("local[2]").appName("testing").getOrCreate()
#data="D:\\data engineering\\BigData\\datasets\\cars.json"
data="D:\\data engineering\\BigData\\datasets\\world_bank.json"
df=spark.read.format("json").load(data)
df.createOrReplaceTempView("tab")
#res=spark.sql("select make , count(*) as cnt from tab group by make order by cnt desc ")
#df.printSchema()
#res.show()
#df.show()

def flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif (type(complex_fields[col_name]) == ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))

        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df
df2=df.withColumnRenamed( "_id_$oid", "idoid" )
df2 = flatten(df)
df2.printSchema()
df2.show()