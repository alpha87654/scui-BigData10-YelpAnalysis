from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim, count

spark = SparkSession.builder.getOrCreate()

checkin_exploded = spark.table("yelp.checkin").select(
    col("business_id"),
    explode(split(col("date"), ",")).alias("checkin_time")
).select(
    col("business_id"),
    trim(col("checkin_time")).alias("checkin_time")
)

most_popular_city_checkins = (
    checkin_exploded.alias("c")
    .join(spark.table("yelp.business").alias("b"), col("c.business_id") == col("b.business_id"))
    .groupBy(col("b.city"))
    .agg(count("*").alias("total_checkins"))
    .filter(col("city").isNotNull() & (trim(col("city")) != ""))
    .orderBy(col("total_checkins").desc())
)

most_popular_city_checkins.show(10, False)