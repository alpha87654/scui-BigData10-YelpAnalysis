%pyspark
user_yearly = (
    review_df
    .withColumn("year", F.year(F.col("rev_date")))
    .groupBy("year", "rev_user_id")
    .agg(F.count("review_id").alias("yearly_count"))
)

window_spec = Window.partitionBy("year").orderBy(F.desc("yearly_count"))

result = (
    user_yearly
    .withColumn("rank", F.dense_rank().over(window_spec))
    .filter(F.col("rank") <= 10)
    .join(
        users_df.select("user_id", "user_name"),
        F.col("rev_user_id") == F.col("user_id"),
        "left"
    )
    .select("year", "user_name", "rev_user_id", "yearly_count", "rank")
    .orderBy("year", "rank")
)
print("=== Top 10 Reviewers Per Year ===")
z.show(result)