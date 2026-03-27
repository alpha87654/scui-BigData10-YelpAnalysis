%pyspark

from pyspark.sql.functions import year, col, count, when, round as _round

# Extract year from yelping_since
user_yearly = user_df.withColumn("year", year(col("yelping_since")))

# Group by year, count total and silent users
yearly_stats = user_yearly.groupBy("year").agg(
    count("*").alias("total_users"),
    count(when(col("review_count") == 0, 1)).alias("silent_users")
).withColumn(
    "silent_proportion_%", _round(col("silent_users") / col("total_users") * 100, 2)
).orderBy("year")

yearly_stats.show(30)
z.show(yearly_stats)