%pyspark
from pyspark.sql.functions import col, when, count, avg, round as _round, sum as _sum

# Clean postal codes in business_df (keep only 5 digits)
business_clean = business_df \
    .filter(col("postal_code").isNotNull()) \
    .withColumn("zip_code", col("postal_code").substr(1, 5))

# Join with Census income data
business_income = business_clean.join(
    census_df, on="zip_code", how="inner"
)

# Add income level segments
business_income = business_income.withColumn(
    "income_level",
    when(col("median_income") < 40000, "Low Income")
    .when(col("median_income") < 75000, "Middle Income")
    .when(col("median_income") < 120000, "High Income")
    .otherwise("Very High Income")
)

print("✅ Businesses matched with Census data:", business_income.count())
print("\nIncome level distribution:")
business_income.groupBy("income_level") \
    .agg(count("business_id").alias("business_count")) \
    .orderBy("business_count", ascending=False) \
    .show()
