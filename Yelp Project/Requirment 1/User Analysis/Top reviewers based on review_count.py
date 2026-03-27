%pyspark
result = (
    users_df
    .select("user_id", "user_name", "user_review_count")
    .orderBy(F.desc("user_review_count"))
    .limit(20)
)
print("=== Top 20 Reviewers by Review Count ===")
z.show(result)