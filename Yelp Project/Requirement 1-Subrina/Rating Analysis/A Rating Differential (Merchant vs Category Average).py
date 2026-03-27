
%pyspark
from pyspark.sql.functions import col, explode, split, avg, round as _round

# Explode categories so each business-category is one row
business_exploded = business_df \
    .filter(col("categories").isNotNull()) \
    .withColumn("category", explode(split(col("categories"), ", ")))

# Calculate average stars per category per city
category_city_avg = business_exploded.groupBy("city", "category") \
    .agg(_round(avg("stars"), 2).alias("category_avg_stars"))

# Join back to get each business's rating vs its category average in same city
rating_diff = business_exploded.join(
    category_city_avg,
    on=["city", "category"],
    how="inner"
).withColumn(
    "rating_differential",
    _round(col("stars") - col("category_avg_stars"), 2)
)

# Top businesses ABOVE their category average (hidden gems)
print("=== Top 20 Businesses ABOVE Category Average (Hidden Gems) ===")
above_avg = rating_diff.filter(col("rating_differential") > 0) \
    .select("name", "city", "category", "stars", "category_avg_stars", "rating_differential") \
    .orderBy("rating_differential", ascending=False) \
    .limit(20)
above_avg.show(20, truncate=40)
z.show(above_avg)

# Top businesses BELOW their category average (underperformers)
print("=== Top 20 Businesses BELOW Category Average (Underperformers) ===")
below_avg = rating_diff.filter(col("rating_differential") < 0) \
    .select("name", "city", "category", "stars", "category_avg_stars", "rating_differential") \
    .orderBy("rating_differential", ascending=True) \
    .limit(20)
below_avg.show(20, truncate=40)
z.show(below_avg)