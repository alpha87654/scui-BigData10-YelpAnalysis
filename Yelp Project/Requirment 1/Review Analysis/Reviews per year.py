%pyspark
result = (
    review_df
    .withColumn("year", F.year(F.col("rev_date")))
    .groupBy("year")
    .agg(F.count("review_id").alias("review_count"))
    .orderBy("year")
)
print("=== Reviews Per Year ===")
z.show(result)