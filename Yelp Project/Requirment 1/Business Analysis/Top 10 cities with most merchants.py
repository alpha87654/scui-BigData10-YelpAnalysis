%pyspark
result = (
    business_df
    .groupBy("city", "state")
    .agg(F.count("business_id").alias("merchant_count"))
    .orderBy(F.desc("merchant_count"))
    .limit(10)
)
print("=== Top 10 Cities with Most Merchants ===")
z.show(result)