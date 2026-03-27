%pyspark

from pyspark.sql.functions import year, col, when

# Add join year
user_with_year = user_df.withColumn("join_year", year(col("yelping_since")))

# Early adopters = joined in 2004-2006 AND review_count in top 25%
threshold = user_with_year.approxQuantile("review_count", [0.75], 0.01)[0]

early_adopters = user_with_year.filter(
    (col("join_year") <= 2006) & (col("review_count") >= threshold)
).select("name", "join_year", "review_count", "fans", "average_stars") \
 .orderBy("review_count", ascending=False)

print(f"Review count threshold (top 25%): {threshold}")
print(f"Number of early adopters (tastemakers): {early_adopters.count()}")
early_adopters.show(20)
z.show(early_adopters)