%pyspark
daily_reviews = (
    philly_reviews
    .groupBy("rev_date")
    .agg(
        F.count("review_id").alias("total_reviews"),
        F.round(F.avg("rev_stars"), 3).alias("avg_stars"),
        F.sum(F.when(F.col("rev_stars") == 1, 1).otherwise(0)).alias("one_star_count"),
        F.sum(F.when(F.col("rev_stars") == 5, 1).otherwise(0)).alias("five_star_count")
    )
    .withColumn("one_star_pct",
        F.round(F.col("one_star_count") / F.col("total_reviews") * 100, 2))
)

reviews_weather = (
    daily_reviews
    .join(weather_df, daily_reviews["rev_date"] == weather_df["weather_date"])
    .select("rev_date", "weather_label", "temp_max_c", "precip_mm",
            "wind_speed", "total_reviews", "avg_stars",
            "one_star_count", "one_star_pct")
)

print("=== Avg Ratings by Weather Condition ===")
z.show(
    reviews_weather
    .groupBy("weather_label")
    .agg(
        F.count("rev_date").alias("days"),
        F.round(F.avg("total_reviews"), 1).alias("avg_daily_reviews"),
        F.round(F.avg("avg_stars"), 3).alias("avg_rating"),
        F.round(F.avg("one_star_pct"), 2).alias("avg_1star_pct")
    )
    .orderBy(F.desc("avg_1star_pct"))
)