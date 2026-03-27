from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim, to_timestamp, date_format, count, lag, round
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

checkin_monthly = spark.table("yelp.checkin").select(
    col("business_id"),
    explode(split(col("date"), ",")).alias("checkin_time")
).select(
    col("business_id"),
    trim(col("checkin_time")).alias("checkin_time")
).select(
    col("business_id"),
    to_timestamp(col("checkin_time"), "yyyy-MM-dd HH:mm:ss").alias("checkin_timestamp")
).select(
    col("business_id"),
    date_format(col("checkin_timestamp"), "yyyy-MM").alias("year_month")
)

top50_restaurants = (
    checkin_monthly
    .groupBy("business_id")
    .agg(count("*").alias("total_checkins"))
    .orderBy(col("total_checkins").desc())
    .limit(50)
)

monthly_counts = (
    checkin_monthly.alias("c")
    .join(top50_restaurants.alias("t"), "business_id")
    .groupBy("business_id", "year_month")
    .agg(count("*").alias("monthly_checkins"))
)

window_spec = Window.partitionBy("business_id").orderBy("year_month")

mom_growth = (
    monthly_counts
    .withColumn("previous_month_checkins", lag("monthly_checkins").over(window_spec))
    .withColumn(
        "mom_growth_rate",
        round(
            ((col("monthly_checkins") - col("previous_month_checkins")) / col("previous_month_checkins")) * 100,
            2
        )
    )
    .orderBy("business_id", "year_month")
)

mom_growth.show(50, False)