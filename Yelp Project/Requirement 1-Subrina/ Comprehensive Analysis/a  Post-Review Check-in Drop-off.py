%pyspark
from pyspark.sql.functions import col, split, explode, trim, to_timestamp, \
    count, avg, datediff, min as _min, max as _max, round as _round

# Parse checkin dates — they are comma separated strings
checkin_parsed = checkin_df \
    .withColumn("date_array", split(col("date"), ", ")) \
    .withColumn("checkin_date", explode(col("date_array"))) \
    .withColumn("checkin_date", trim(col("checkin_date"))) \
    .withColumn("checkin_ts", to_timestamp(col("checkin_date"), "yyyy-MM-dd HH:mm:ss")) \
    .select("business_id", "checkin_ts")

# Get first and last review date per business
review_dates = review_df.groupBy("business_id").agg(
    _min("date").alias("first_review_date"),
    _max("date").alias("last_review_date"),
    count("*").alias("total_reviews")
)

# Get checkin counts before and after first review
checkin_with_review = checkin_parsed.join(
    review_dates, on="business_id", how="inner"
)

# Count checkins before vs after first review
from pyspark.sql.functions import when, sum as _sum

checkin_before_after = checkin_with_review.withColumn(
    "period",
    when(col("checkin_ts") < col("first_review_date").cast("timestamp"), "before_first_review")
    .otherwise("after_first_review")
).groupBy("business_id", "period") \
 .agg(count("*").alias("checkin_count"))

# Pivot to get before/after columns side by side
from pyspark.sql.functions import lit
pivot_df = checkin_before_after.groupBy("business_id").pivot("period") \
    .agg(count("checkin_count")) \
    .fillna(0)

# Calculate drop-off
dropoff_df = pivot_df.withColumn(
    "dropoff_%",
    _round(
        (col("before_first_review") - col("after_first_review")) /
        (col("before_first_review") + lit(1)) * 100, 2
    )
).orderBy("dropoff_%", ascending=False)

print("=== Post-Review Check-in Drop-off Analysis ===")
dropoff_df.show(20, truncate=False)
z.show(dropoff_df.limit(20))

# Summary stats
print("\n=== Summary ===")
dropoff_df.agg(
    avg("dropoff_%").alias("avg_dropoff_%"),
    avg("before_first_review").alias("avg_checkins_before"),
    avg("after_first_review").alias("avg_checkins_after")
).show()