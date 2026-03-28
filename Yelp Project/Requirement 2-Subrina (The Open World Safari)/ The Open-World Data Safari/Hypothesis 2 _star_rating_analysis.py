%pyspark
from pyspark.sql.functions import col, count, avg, round as _round

# Filter restaurants only
restaurant_income = business_income.filter(
    col("categories").isNotNull() & col("categories").contains("Restaurant")
)

# Average stars and reviews by income level
rating_analysis = restaurant_income.groupBy("income_level").agg(
    count("business_id").alias("restaurant_count"),
    _round(avg("stars"), 2).alias("avg_stars"),
    _round(avg("review_count"), 2).alias("avg_review_count"),
    _round(avg("median_income"), 0).alias("avg_zip_income")
).orderBy("avg_stars", ascending=False)

print("=== Hypothesis Test 2: Restaurant Ratings by Income Level ===")
rating_analysis.show(truncate=False)
z.show(rating_analysis)