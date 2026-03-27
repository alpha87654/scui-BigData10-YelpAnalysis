from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

review_length_correlation = spark.sql("""
SELECT
    ROUND(CORR(LENGTH(text), stars), 4) AS review_length_rating_correlation
FROM yelp.review
WHERE text IS NOT NULL
  AND stars IS NOT NULL
""")

review_length_correlation.show(20, False)