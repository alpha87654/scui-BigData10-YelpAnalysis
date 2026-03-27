%pyspark
from pyspark.sql.functions import sum as _sum

chinese_reviews = business_df.filter(
    col("categories").isNotNull() & col("categories").contains("Chinese")
).agg(_sum("review_count")).collect()[0][0]

american_reviews = business_df.filter(
    col("categories").isNotNull() & (
        col("categories").contains("American (Traditional)") |
        col("categories").contains("American (New)")
    )
).agg(_sum("review_count")).collect()[0][0]

mexican_reviews = business_df.filter(
    col("categories").isNotNull() & col("categories").contains("Mexican")
).agg(_sum("review_count")).collect()[0][0]

print(f"Total reviews - Chinese restaurants:  {chinese_reviews}")
print(f"Total reviews - American restaurants: {american_reviews}")
print(f"Total reviews - Mexican restaurants:  {mexican_reviews}")