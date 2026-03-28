%pyspark
historical = (
    review_df
    .groupBy("rev_business_id")
    .agg(F.avg("rev_stars").alias("historical_avg"))
)

recent = (
    review_df
    .filter(F.datediff(F.current_date(), F.col("rev_date")) <= 365)
    .groupBy("rev_business_id")
    .agg(
        F.avg("rev_stars").alias("recent_avg"),
        F.count("review_id").alias("recent_count")
    )
)

result = (
    historical.alias("h")
    .join(recent.alias("r"), "rev_business_id")
    .filter(
        (F.col("r.recent_avg") - F.col("h.historical_avg") >= 1.0) &
        (F.col("r.recent_count") >= 5)
    )
    .join(
        business_df.select("business_id", "name", "city", "state"),
        F.col("rev_business_id") == F.col("business_id")
    )
    .select(
        "name", "city", "state",
        F.round("historical_avg", 2).alias("historical_avg"),
        F.round("recent_avg", 2).alias("recent_avg"),
        F.round(F.col("recent_avg") - F.col("historical_avg"), 2).alias("improvement"),
        "recent_count"
    )
    .orderBy(F.desc("improvement"))
)
print("=== Turnaround Merchants ===")
z.show(result)
