%pyspark
from pyspark.sql.functions import split, explode, trim, col

# categories is a comma-separated string, so we split it into array first
categories_df = business_df.filter(col("categories").isNotNull()) \
    .select(explode(split(col("categories"), ", ")).alias("category")) \
    .select(trim(col("category")).alias("category"))

# Count distinct categories
distinct_count = categories_df.distinct().count()
print("Number of distinct categories:", distinct_count)

# Top 20 most common
print("\nTop 20 most common categories:")
categories_df.groupBy("category").count().orderBy("count", ascending=False).show(20, truncate=False)
z.show(categories_df.groupBy("category").count().orderBy("count", ascending=False))