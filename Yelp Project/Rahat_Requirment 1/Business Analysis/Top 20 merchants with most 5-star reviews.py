%pyspark
result = (
    review_df
    .filter(F.col("rev_stars") == 5)
    .groupBy("rev_business_id")
    .agg(F.count("review_id").alias("five_star_count"))
    .join(
        business_df.select("business_id", "name", "city", "state"),
        F.col("rev_business_id") == F.col("business_id")
    )
    .select("name", "city", "state", "five_star_count")
    .orderBy(F.desc("five_star_count"))
    .limit(20)
)
print("=== Top 20 Merchants with Most 5-Star Reviews ===")
z.show(result)