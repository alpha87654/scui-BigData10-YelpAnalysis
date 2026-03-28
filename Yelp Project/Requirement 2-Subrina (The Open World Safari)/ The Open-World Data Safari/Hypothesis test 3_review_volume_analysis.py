%pyspark
from pyspark.sql.functions import col, count, avg, round as _round, year

# Join reviews with business_income
review_income = review_df.join(
    business_income.select("business_id", "income_level", "median_income"),
    on="business_id", how="inner"
)

# Review volume by income level per year
review_by_income_year = review_income \
    .withColumn("year", year(col("date"))) \
    .filter(col("year") >= 2015) \
    .groupBy("year", "income_level") \
    .agg(
        count("review_id").alias("review_count"),
        _round(avg("stars"), 2).alias("avg_rating")
    ).orderBy("year", "income_level")

print("=== Hypothesis Test 3: Review Volume & Rating by Income Level (2015-2022) ===")
review_by_income_year.show(40, truncate=False)
z.show(review_by_income_year)