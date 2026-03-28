%pyspark
# Find users who reviewed multiple suspicious businesses
coordinated_users = (
    high_ghost
    .groupBy("rev_user_id", "user_name")
    .agg(
        F.countDistinct("rev_business_id").alias("suspicious_biz_reviewed"),
        F.count("review_id").alias("total_ghost_reviews"),
        F.round(F.avg("rev_stars"), 2).alias("avg_stars_given")
    )
    .filter(F.col("suspicious_biz_reviewed") >= 2)
    .orderBy(F.desc("suspicious_biz_reviewed"))
)

print("=== Coordinated Ghost Users (reviewed 2+ suspicious businesses) ===")
z.show(coordinated_users)