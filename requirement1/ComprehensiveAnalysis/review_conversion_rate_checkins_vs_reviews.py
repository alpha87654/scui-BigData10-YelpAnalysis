from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim, count, round

spark = SparkSession.builder.getOrCreate()

checkin_counts_filtered = spark.table("yelp.checkin").select(
    col("business_id"),
    explode(split(col("date"), ",")).alias("checkin_time")
).select(
    col("business_id"),
    trim(col("checkin_time")).alias("checkin_time")
).groupBy("business_id").agg(
    count("*").alias("total_checkins")
).filter(
    col("total_checkins") >= 50
)

review_counts_filtered = spark.table("yelp.review").groupBy("business_id").agg(
    count("*").alias("total_reviews")
)

review_conversion_rate_filtered = (
    checkin_counts_filtered.alias("c")
    .join(review_counts_filtered.alias("r"), "business_id")
    .select(
        col("business_id"),
        col("total_checkins"),
        col("total_reviews"),
        round(col("total_reviews") / col("total_checkins"), 4).alias("review_conversion_rate")
    )
    .orderBy(col("review_conversion_rate").desc())
)

review_conversion_rate_filtered.show(20, False)