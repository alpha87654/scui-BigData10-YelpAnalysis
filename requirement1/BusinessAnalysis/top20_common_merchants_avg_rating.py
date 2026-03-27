from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

top20_merchants = spark.sql("""
SELECT
    name,
    COUNT(*) AS merchant_count,
    ROUND(AVG(stars), 2) AS average_rating
FROM yelp.business
WHERE name IS NOT NULL
  AND TRIM(name) <> ''
  AND state IS NOT NULL
GROUP BY name
ORDER BY merchant_count DESC, average_rating DESC
LIMIT 20
""")

top20_merchants.show(20, False)