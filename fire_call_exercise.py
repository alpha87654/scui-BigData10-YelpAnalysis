from mailcap import show

import agg
from certifi import where
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_timestamp, desc, avg, count
from pyspark.sql.types import *


# Start Spark
spark = (SparkSession.builder
         .appName("SF Fire Calls Step 1")
         .master("local[*]")
         .getOrCreate())

# Define schema
fire_schema = StructType([
    StructField('CallNumber', IntegerType(), True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', IntegerType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)
])

# Read file
fire_df = (spark.read
           .option("header", "true")
           .schema(fire_schema)
           .csv("dataset/sf-fire-calls.txt"))

# Show result
# #1.Select "IncidentNumber", "AvailableDtTm", "CallType" from data, and filter the CallType is not "Medical Incident"
# few_fire_df = (fire_df
#  .select("IncidentNumber", "AvailableDtTm", "CallType")
#  .where(col("CallType") != "Medical Incident"))
# few_fire_df.show(5, truncate=False)

#2.how many distinct CallTypes.
# (fire_df
#     .select("CallType")
#     .where(col("CallType").isNotNull())
#     .agg(countDistinct("CallType").alias("DistinctCallTypes"))
#     .show())

#3.list the distinct call types in the data set
# (fire_df
#     .select("CallType")
#     .where(col("CallType").isNotNull())
#     .distinct()
#     .show(50, truncate=False))

#4.Rename Column Delay to "ResponseDelayedinMins" and take a look at the response times that were longer than five
#minutes:

# new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
#
# (new_fire_df
#     .select("ResponseDelayedinMins")
#     .where(col("ResponseDelayedinMins") > 5)
#     .show(5, truncate=False))

# 5. Convert "CallDate", "WatchDate" and "AvailableDtTm" to timestamp date type and how many years’ worth of Fire Department calls
# cleaned_df = (fire_df
#     .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
#     .drop("CallDate")
#     .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
#     .drop("WatchDate")
#     .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
#     .drop("AvailableDtTm"))
#
# cleaned_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, truncate=False)

# 6.Most common types of fire calls
# (fire_df
#     .groupBy("CallType")
#     .count()
#     .orderBy(desc("count"))
#     .show(20, truncate=False))

#7.Sum of alarms, average response time, minimum and maximum response times for fire calls
# from pyspark.sql.functions import col, sum, avg, min, max
#
# new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
#
# fire_calls_df = new_fire_df.where(col("CallType").contains("Fire"))
#
# (fire_calls_df
#     .agg(
#         sum("NumAlarms").alias("TotalAlarms"),
#         avg("ResponseDelayedinMins").alias("AvgResponseTime"),
#         min("ResponseDelayedinMins").alias("MinResponseTime"),
#         max("ResponseDelayedinMins").alias("MaxResponseTime")
#     )
#     .show(truncate=False))

# 8.Different types of fire department calls in 2018
# from pyspark.sql.functions import col, to_timestamp, year
# cleaned_df = (fire_df
#     .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")))
#
# (cleaned_df
#     .where(year("IncidentDate") == 2018)
#     .select("CallType")
#     .where(col("CallType").isNotNull())
#     .distinct()
#     .orderBy("CallType")
#     .show(100, truncate=False))

# 9.Which months in 2018 had the highest number of calls
# from pyspark.sql.functions import col, to_timestamp, year , month
# cleaned_df = (fire_df
#     .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")))
# (cleaned_df
#     .where(year("IncidentDate") == 2018)
#     .groupBy(month("IncidentDate").alias("Month"))
#     .count()
#     .orderBy(desc("count"))
#     .show(12, truncate=False))

#10.Which neighborhood had the most fire calls in 2018
# from pyspark.sql.functions import col, to_timestamp, year , month , weekofyear
# cleaned_df = (fire_df
#     .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")))
#
# (cleaned_df
#     .where(year("IncidentDate") == 2018)
#     .groupBy("Neighborhood")
#     .count()
#     .orderBy(desc("count"))
#     .show(20, truncate=False))

#11.Which neighborhoods had the worst response times in 2018


# (cleaned_df
#     .withColumnRenamed("Delay", "ResponseDelayedinMins")
#     .where(year("IncidentDate") == 2018)
#     .groupBy("Neighborhood")
#     .agg(avg("ResponseDelayedinMins").alias("AvgResponseDelay"))
#     .orderBy(desc("AvgResponseDelay"))
#     .show(20, truncate=False))

#12: Which week in 2018 had the most fire calls
# (cleaned_df
#     .where(year("IncidentDate") == 2018)
#     .groupBy(weekofyear("IncidentDate").alias("Week"))
#     .count()
#     .orderBy(desc("count"))
#     .show(20, truncate=False))

# 13: Correlation between neighborhood, zip code, and number of fire calls
# calls_by_area_df = (cleaned_df
#     .where(year("IncidentDate") == 2018)
#     .groupBy("Neighborhood", "Zipcode")
#     .agg(count("*").alias("NumCalls")))
#
# calls_by_area_df.show(20, truncate=False)

# Step 14A: Save as Parquet
# fire_df.write.parquet("fire_calls_parquet")

parquet_df = spark.read.parquet("fire_calls_parquet")
parquet_df.show(5, truncate=False)

# Stop Spark
spark.stop()