%pyspark
result = (
    users_df
    .withColumn("year", F.year(F.to_date(F.col("user_yelping_since"))))
    .groupBy("year")
    .agg(F.count("user_id").alias("new_users"))
    .orderBy("year")
)
print("=== Users Joining Per Year ===")
z.show(result)