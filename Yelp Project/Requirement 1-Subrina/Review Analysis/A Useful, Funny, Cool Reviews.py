
%pyspark
from pyspark.sql.functions import col, count, when

review_stats = review_df.agg(
    count(when(col("useful") > 0, 1)).alias("useful_reviews"),
    count(when(col("funny") > 0, 1)).alias("funny_reviews"),
    count(when(col("cool") > 0, 1)).alias("cool_reviews"),
    count("*").alias("total_reviews")
)

review_stats.show()

z.show(review_stats)