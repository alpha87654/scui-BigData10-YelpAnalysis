from pyspark.sql.functions import col, when, count, avg, size, split

# Mark users as elite or not (elite string is non-empty)
user_elite = user_df.withColumn(
    "is_elite",
    when(
        col("elite").isNotNull() & (col("elite") != ""), "Elite"
    ).otherwise("Non-Elite")
)

# Compare stats between elite and non-elite
elite_impact = user_elite.groupBy("is_elite").agg(
    count("user_id").alias("user_count"),
    avg("review_count").alias("avg_reviews"),
    avg("average_stars").alias("avg_stars"),
    avg("fans").alias("avg_fans"),
    avg("useful").alias("avg_useful_votes")
)

elite_impact.show()
z.show(elite_impact)