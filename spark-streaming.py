from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


spark = SparkSession  \
        .builder  \
        .appName("Retail_Data_Analysis")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

lines = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","real-time-project") \
    .option("startingOffsets", "latest")  \
        .load()

kafkaDF = lines.selectExpr("cast(value as string)")

schema = StructType([
                StructField("items",ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType())]))),
        StructField("type", StringType(),True),
        StructField("country",StringType(),True),
        StructField("invoice_no",StringType(),True),
        StructField("timestamp",TimestampType(),True)])


kafkaDF1 = kafkaDF.withColumn("value",from_json("value",schema)) \
                  .select("value.items","value.invoice_no","value.country","value.timestamp","value.type")


def get_total_cost(items,type):
    total_cost = 0
    for x in items:
        total_cost = total_cost + x['unit_price']*x['quantity']
    if(type=='RETURN'):
        total_cost = total_cost*(-1)
    return total_cost

totalCostUDF = udf(get_total_cost, DoubleType())

def get_total_items(items):
    total_items = 0
    for x in items:
        total_items = total_items + x['quantity']
    return total_items

totalitems = udf(get_total_items,IntegerType())

def is_order(ordertype):
    if ordertype == 'ORDER':
        return 1
    else:
        return 0

isTypeOrderUDF = udf(is_order,IntegerType())

def is_return(returntype):
    if returntype == 'RETURN':
        return 1
    else:
        return 0

isTypeReturnUDF = udf(is_return,IntegerType())

retail_console_result = kafkaDF1.withColumn("is_order", isTypeOrderUDF(kafkaDF1.type)).withColumn("is_return", isTypeReturnUDF(kafkaDF1.type)) \
                  .withColumn("total_items",totalitems(kafkaDF1.items)).withColumn("total_cost",totalCostUDF(kafkaDF1.items,kafkaDF1.type)) \
                  .select("invoice_no","country","timestamp","total_cost","total_items","is_order","is_return")


retail_console_result_output = retail_console_result \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()


# Calculate time based KPIs
agg_time = retail_console_result \
    .withWatermark("timestamp","1 minute") \
    .groupby(window("timestamp", "1 minute","1 minute")) \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        avg("total_cost").alias("average_transaction_size"),
        avg("is_return").alias("rate_of_return")) \
    .select("window.start","window.end","total_volume_of_sales","average_transaction_size","rate_of_return")


# Calculate time and country based KPIs
agg_time_country = retail_console_result \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute","1 minute"), "country") \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        count("invoice_no").alias("OPM"),
        avg("is_return").alias("rate_of_return")) \
    .select("window.start","window.end","country", "OPM","total_volume_of_sales","rate_of_return")


# Write time based KPI values
by_Time = agg_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_based_data/") \
    .option("checkpointLocation", "time_based_data/cp/") \
    .trigger(processingTime="1 minutes") \
    .start()


# Write time and country based KPI values
by_Time_Country = agg_time_country.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "country_time_based_data/") \
    .option("checkpointLocation", "country_time_based_data/cp/") \
    .trigger(processingTime="1 minutes") \
    .start()

by_Time_Country.awaitTermination()