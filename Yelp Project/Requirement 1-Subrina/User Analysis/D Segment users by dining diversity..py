%pyspark
from pyspark.sql.functions import col, explode, split, count, countDistinct, when

# Join reviews with business to get categories
review_business = review_df.join(
    business_df.select("business_id", "categories"),
    on="business_id", how="inner"
).filter(col("categories").isNotNull())

# Explode categories per review
user_categories = review_business.withColumn(
    "category", explode(split(col("categories"), ", "))
)

# Count distinct categories per user
diversity = user_categories.groupBy("user_id").agg(
    countDistinct("category").alias("distinct_categories"),
    count("review_id").alias("total_reviews")
)

# Segment into Low / Medium / High diversity
diversity_segmented = diversity.withColumn(
    "diversity_segment",
    when(col("distinct_categories") <= 5, "Low")
    .when(col("distinct_categories") <= 15, "Medium")
    .otherwise("High")
)

print("=== Diversity Segment Summary ===")
diversity_segmented.groupBy("diversity_segment") \
    .agg(count("user_id").alias("user_count")) \
    .orderBy("user_count", ascending=False).show()

print("\n=== Top 20 Most Diverse Users ===")
diversity_segmented.orderBy("distinct_categories", ascending=False).show(20)
