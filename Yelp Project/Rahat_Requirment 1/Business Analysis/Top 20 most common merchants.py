%pyspark
result = (
    business_df
    .groupBy("name")
    .agg(F.count("business_id").alias("location_count"))
    .orderBy(F.desc("location_count"))
    .limit(20)
)
print("=== Top 20 Most Common Merchants ===")
z.show(result)