%pyspark
# Task 1: Busisness Analysis : (B):Count the number of restaurant types (Chinese, American, Mexican).
from pyspark.sql.functions import col

# Since categories is a string, we use contains()
chinese_count = business_df.filter(
    col("categories").isNotNull() & col("categories").contains("Chinese")
).count()

american_count = business_df.filter(
    col("categories").isNotNull() & (
        col("categories").contains("American (Traditional)") |
        col("categories").contains("American (New)")
    )
).count()

mexican_count = business_df.filter(
    col("categories").isNotNull() & col("categories").contains("Mexican")
).count()

print(f"Chinese restaurants:  {chinese_count}")
print(f"American restaurants: {american_count}")
print(f"Mexican restaurants:  {mexican_count}")