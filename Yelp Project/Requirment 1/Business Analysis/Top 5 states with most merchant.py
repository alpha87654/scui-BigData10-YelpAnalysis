%pyspark
result = (
    business_df
    .groupBy("state")
    .agg(F.count("business_id").alias("merchant_count"))
    .orderBy(F.desc("merchant_count"))
    .limit(5)
)
print("=== Top 5 States with Most Merchants ===")
z.show(result)