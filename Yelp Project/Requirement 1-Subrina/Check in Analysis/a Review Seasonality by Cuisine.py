%pyspark
from pyspark.sql.functions import col, month, count, explode, split, avg

# Join reviews with business to get cuisine categories
review_with_cuisine = review_df.join(
    business_df.select("business_id", "categories"),
    on="business_id", how="inner"
).filter(col("categories").isNotNull())

# Focus on 3 main cuisine types
cuisine_keywords = {
    "Chinese": "Chinese",
    "Mexican": "Mexican",
    "American": "American (Traditional)"
}

# Get monthly review counts per cuisine
from pyspark.sql.functions import when, lit

review_with_cuisine = review_with_cuisine.withColumn(
    "cuisine",
    when(col("categories").contains("Chinese"), "Chinese")
    .when(col("categories").contains("Mexican"), "Mexican")
    .when(col("categories").contains("American (Traditional)") |
          col("categories").contains("American (New)"), "American")
    .otherwise(None)
).filter(col("cuisine").isNotNull())

# Monthly seasonality
seasonality = review_with_cuisine \
    .withColumn("month", month(col("date"))) \
    .groupBy("month", "cuisine") \
    .agg(count("*").alias("review_count")) \
    .orderBy("cuisine", "month")

print("=== Review Seasonality by Cuisine (Monthly) ===")
seasonality.show(40, truncate=False)
z.show(seasonality)