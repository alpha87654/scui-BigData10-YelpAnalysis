%pyspark
review_freq = (
    review_df
    .groupBy("rev_business_id")
    .agg(F.count("review_id").alias("review_count"))
)

avg_rating = (
    review_df
    .groupBy("rev_business_id")
    .agg(F.round(F.avg("rev_stars"), 3).alias("avg_rating"))
)

checkin_count = (
    checkin_exploded
    .groupBy("business_id")
    .agg(F.count("*").alias("checkin_count"))
)

combined = (
    business_df.select("business_id", "name", "city", "state")
    .join(review_freq,
          F.col("business_id") == F.col("rev_business_id"), "left")
    .join(avg_rating,
          F.col("business_id") == avg_rating["rev_business_id"], "left")
    .join(checkin_count, "business_id", "left")
    .fillna(0, subset=["review_count", "avg_rating", "checkin_count"])
)

city_win = Window.partitionBy("city")

scored = (
    combined
    .withColumn("r_pct", F.percent_rank().over(city_win.orderBy("review_count")))
    .withColumn("s_pct", F.percent_rank().over(city_win.orderBy("avg_rating")))
    .withColumn("c_pct", F.percent_rank().over(city_win.orderBy("checkin_count")))
    .withColumn("score",
                F.round((F.col("r_pct") + F.col("s_pct") + F.col("c_pct")) / 3, 4))
)

rank_win = Window.partitionBy("city").orderBy(F.desc("score"))

result = (
    scored
    .withColumn("city_rank", F.row_number().over(rank_win))
    .filter(F.col("city_rank") <= 5)
    .select("city", "state", "city_rank", "name",
            "review_count", "avg_rating", "checkin_count", "score")
    .orderBy("city", "city_rank")
)
print("=== Top 5 Merchants Per City ===")
z.show(result)