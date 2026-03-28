%pyspark
total = review_df.count()
result = (
    review_df
    .groupBy("rev_stars")
    .agg(F.count("review_id").alias("review_count"))
    .withColumn("percentage", F.round(F.col("review_count") / total * 100, 2))
    .orderBy("rev_stars")
)
print("=== Rating Distribution (1–5 Stars) ===")
z.show(result)