from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

top10_cities_highest_ratings = spark.sql("""
SELECT
    city,
    ROUND(AVG(stars), 2) AS average_rating,
    COUNT(*) AS business_count
FROM yelp.business
WHERE city IS NOT NULL
  AND TRIM(city) <> ''
GROUP BY city
HAVING COUNT(*) >= 10
ORDER BY average_rating DESC, business_count DESC
LIMIT 10
""")

top10_cities_highest_ratings.show(10, False)