%pyspark
# For top 10 most suspicious businesses, show daily review pattern
top10_ids = [r["business_id"] for r in suspicious_biz.limit(10).collect()]

daily_pattern = (
    review_df
    .filter(F.col("rev_business_id").isin(top10_ids))
    .withColumn("rev_date", F.to_date(F.col("rev_date")))
    .groupBy("rev_business_id", "rev_date")
    .agg(
        F.count("review_id").alias("daily_reviews"),
        F.sum(F.when(F.col("rev_stars") == 5, 1).otherwise(0)).alias("daily_5star"),
        F.round(F.avg("rev_stars"), 2).alias("daily_avg_stars")
    )
    .withColumn("five_star_ratio",
                F.round(F.col("daily_5star") / F.col("daily_reviews") * 100, 1))
    .orderBy("rev_business_id", "rev_date")
)

print("=== Daily Review Pattern for Top 10 Suspicious Businesses ===")
z.show(daily_pattern)