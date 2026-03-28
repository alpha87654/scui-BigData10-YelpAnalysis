%pyspark
from pyspark.sql.functions import col, count, avg, round as _round, when, sum as _sum

# Calculate survival rate per income level
survival_analysis = business_income.groupBy("income_level").agg(
    count("business_id").alias("total_businesses"),
    _sum(when(col("is_open") == 1, 1).otherwise(0)).alias("open_businesses"),
    _sum(when(col("is_open") == 0, 1).otherwise(0)).alias("closed_businesses"),
    _round(avg("stars"), 2).alias("avg_stars"),
    _round(avg("review_count"), 2).alias("avg_reviews")
).withColumn(
    "survival_rate_%",
    _round(col("open_businesses") / col("total_businesses") * 100, 2)
).orderBy("survival_rate_%", ascending=False)

print("=== Hypothesis Test 1: Restaurant Survival Rate by Income Level ===")
survival_analysis.show(truncate=False)
z.show(survival_analysis)