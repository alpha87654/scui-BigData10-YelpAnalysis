%pyspark
# Get the spike date (date of max 5-star cluster) per business
spike_dates = (
    spike_window
    .groupBy("business_id")
    .agg(F.max("window_start").alias("spike_date"))
)

# Join reviews with spike dates
reviews_with_spike = (
    review_df
    .join(
        spike_dates,
        review_df["rev_business_id"] == spike_dates["business_id"]
    )
    .withColumn("rev_date", F.to_date(F.col("rev_date")))
    .withColumn("spike_date", F.to_date(F.col("spike_date")))
    .withColumn("days_from_spike",
                F.datediff(F.col("rev_date"), F.col("spike_date")))
)

# Before spike: -180 to -1 days | After spike: +1 to +180 days
before_spike = (
    reviews_with_spike
    .filter(F.col("days_from_spike").between(-180, -1))
    .groupBy("business_id")
    .agg(
        F.round(F.avg("rev_stars"), 3).alias("avg_before"),
        F.count("review_id").alias("reviews_before")
    )
)

after_spike = (
    reviews_with_spike
    .filter(F.col("days_from_spike").between(1, 180))
    .groupBy("business_id")
    .agg(
        F.round(F.avg("rev_stars"), 3).alias("avg_after"),
        F.count("review_id").alias("reviews_after")
    )
)

lifecycle = (
    before_spike
    .join(after_spike, "business_id")
    .withColumn("rating_change",
                F.round(F.col("avg_after") - F.col("avg_before"), 3))
    .withColumn("review_volume_change",
                F.col("reviews_after") - F.col("reviews_before"))
    .join(
        business_df.select("business_id", "name", "city", "state"),
        "business_id"
    )
    .select(
        "name", "city", "state",
        "avg_before", "avg_after", "rating_change",
        "reviews_before", "reviews_after", "review_volume_change"
    )
    .orderBy("rating_change")  # worst backlash first
)

print("=== Rating Before vs After Spike (Backlash Analysis) ===")
z.show(lifecycle)

print("\n=== Summary: Did fake reviews backfire? ===")
z.show(
    lifecycle
    .agg(
        F.round(F.avg("rating_change"), 3).alias("avg_rating_change"),
        F.sum(F.when(F.col("rating_change") < 0, 1).otherwise(0))
        .alias("businesses_with_backlash"),
        F.sum(F.when(F.col("rating_change") > 0, 1).otherwise(0))
        .alias("businesses_improved"),
        F.round(F.avg("review_volume_change"), 1).alias("avg_volume_change")
    )
)
