%pyspark
before_dist = (
    reviews_with_spike
    .filter(F.col("days_from_spike").between(-180, -1))
    .groupBy("business_id")
    .agg(
        F.sum(F.when(F.col("rev_stars") == 1, 1).otherwise(0)).alias("one_star_before"),
        F.count("review_id").alias("total_before")
    )
    .withColumn("one_star_pct_before",
                F.round(F.col("one_star_before") / F.col("total_before") * 100, 2))
)

after_dist = (
    reviews_with_spike
    .filter(F.col("days_from_spike").between(1, 180))
    .groupBy("business_id")
    .agg(
        F.sum(F.when(F.col("rev_stars") == 1, 1).otherwise(0)).alias("one_star_after"),
        F.count("review_id").alias("total_after")
    )
    .withColumn("one_star_pct_after",
                F.round(F.col("one_star_after") / F.col("total_after") * 100, 2))
)

backlash = (
    before_dist
    .join(after_dist, "business_id")
    .withColumn("one_star_increase",
                F.round(F.col("one_star_pct_after") - F.col("one_star_pct_before"), 2))
    .join(
        business_df.select("business_id", "name", "city"),
        "business_id"
    )
    .select("name", "city",
            "one_star_pct_before", "one_star_pct_after", "one_star_increase")
    .orderBy(F.desc("one_star_increase"))
    .limit(20)
)

print("=== 1-Star Review % Before vs After (Backlash from Real Customers) ===")
z.show(backlash)