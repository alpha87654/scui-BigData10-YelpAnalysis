%pyspark
result = (
    users_df
    .select("user_id", "user_name", "user_fans", "user_review_count")
    .orderBy(F.desc("user_fans"))
    .limit(20)
)
print("=== Top 20 Most Popular Users by Fans ===")
z.show(result)