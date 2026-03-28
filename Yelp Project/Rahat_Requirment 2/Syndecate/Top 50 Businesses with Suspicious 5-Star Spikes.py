%pyspark
# Get all 5-star reviews with dates
five_star = (
    review_df
    .filter(F.col("rev_stars") == 5)
    .select("rev_business_id", "rev_user_id", "rev_date")
    .withColumn("rev_date", F.to_date(F.col("rev_date")))
)

# Self-join to count 5-star reviews within 30 days of each review
spike_window = (
    five_star.alias("a")
    .join(
        five_star.alias("b"),
        (F.col("a.rev_business_id") == F.col("b.rev_business_id")) &
        (F.datediff(F.col("b.rev_date"), F.col("a.rev_date")).between(0, 30))
    )
    .groupBy(
        F.col("a.rev_business_id").alias("business_id"),
        F.col("a.rev_date").alias("window_start")
    )
    .agg(F.count("b.rev_user_id").alias("reviews_in_30_days"))
    .filter(F.col("reviews_in_30_days") >= 10)
)

# Get max spike per business
max_spike = (
    spike_window
    .groupBy("business_id")
    .agg(
        F.max("reviews_in_30_days").alias("max_spike_count"),
        F.count("window_start").alias("spike_periods")
    )
)

# Join with business info
suspicious_biz = (
    max_spike
    .join(
        business_df.select("business_id", "name", "city", "state",
                           "stars", "review_count", "categories"),
        "business_id"
    )
    .orderBy(F.desc("max_spike_count"))
    .limit(50)
)

print("=== Top 50 Businesses with Suspicious 5-Star Spikes ===")
z.show(suspicious_biz)

# Cache for reuse in later cells
suspicious_biz.cache()
suspicious_ids = [r["business_id"] for r in suspicious_biz.collect()]
print(f"\nTotal suspicious businesses found: {len(suspicious_ids)}")