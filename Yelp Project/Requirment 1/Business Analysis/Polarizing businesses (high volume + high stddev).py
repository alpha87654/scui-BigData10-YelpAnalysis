%pyspark
result = (
    review_df
    .groupBy("rev_business_id")
    .agg(
        F.count("review_id").alias("review_count"),
        F.round(F.avg("rev_stars"), 2).alias("avg_stars"),
        F.round(F.stddev("rev_stars"), 3).alias("rating_stddev")
    )
    .filter(F.col("review_count") >= 100)
    .join(
        business_df.select("business_id", "name", "city", "state"),
        F.col("rev_business_id") == F.col("business_id")
    )
    .select("name", "city", "state", "review_count", "avg_stars", "rating_stddev")
    .orderBy(F.desc("rating_stddev"))
    .limit(20)
)
print("=== Top 20 Most Polarizing Businesses ===")
z.show(result)