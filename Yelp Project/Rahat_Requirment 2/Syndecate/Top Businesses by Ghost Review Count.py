%pyspark
# Get all reviews for suspicious businesses
suspicious_reviews = (
    review_df
    .filter(F.col("rev_business_id").isin(suspicious_ids))
    .select("review_id", "rev_business_id", "rev_user_id",
            "rev_stars", "rev_date")
)

# Join with user data to get account age and review history
reviews_with_users = (
    suspicious_reviews
    .join(
        users_df.select(
            "user_id", "user_name", "user_review_count",
            "user_yelping_since", "user_fans", "user_elite"
        ),
        suspicious_reviews["rev_user_id"] == users_df["user_id"]
    )
    .withColumn("rev_date", F.to_date(F.col("rev_date")))
    .withColumn("join_date", F.to_date(F.col("user_yelping_since")))
    .withColumn("account_age_days",
                F.datediff(F.col("rev_date"), F.col("join_date")))
)

# Flag ghost account characteristics:
# 1. Account age < 30 days when review was written
# 2. Total review count <= 3 (barely active)
# 3. Gave 5 stars
# 4. No elite status ever
# 5. Zero fans
ghost_flags = (
    reviews_with_users
    .withColumn("is_new_account",
                F.when(F.col("account_age_days") < 30, 1).otherwise(0))
    .withColumn("is_low_activity",
                F.when(F.col("user_review_count") <= 3, 1).otherwise(0))
    .withColumn("is_five_star",
                F.when(F.col("rev_stars") == 5, 1).otherwise(0))
    .withColumn("is_no_elite",
                F.when(
                    F.col("user_elite").isNull() |
                    (F.col("user_elite") == "") |
                    (F.col("user_elite") == "None"), 1
                ).otherwise(0))
    .withColumn("is_no_fans",
                F.when(F.col("user_fans") == 0, 1).otherwise(0))
    # Ghost score: sum of all flags (max = 5)
    .withColumn("ghost_score",
                F.col("is_new_account") + F.col("is_low_activity") +
                F.col("is_five_star") + F.col("is_no_elite") +
                F.col("is_no_fans"))
)

# Reviews with ghost score >= 4 are highly suspicious
high_ghost = ghost_flags.filter(F.col("ghost_score") >= 4)

print("=== Ghost Account Review Distribution ===")
z.show(
    ghost_flags
    .groupBy("ghost_score")
    .agg(F.count("review_id").alias("review_count"))
    .orderBy("ghost_score")
)

print("\n=== Top Businesses by Ghost Review Count ===")
z.show(
    high_ghost
    .groupBy("rev_business_id")
    .agg(
        F.count("review_id").alias("ghost_review_count"),
        F.countDistinct("rev_user_id").alias("ghost_user_count")
    )
    .join(
        business_df.select("business_id", "name", "city", "state"),
        F.col("rev_business_id") == F.col("business_id")
    )
    .select("name", "city", "state", "ghost_review_count", "ghost_user_count")
    .orderBy(F.desc("ghost_review_count"))
    .limit(20)
)