from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

restaurant_rating_distribution = spark.sql("""
SELECT
    CASE
        WHEN categories LIKE '%Chinese%' THEN 'Chinese'
        WHEN categories LIKE '%American%' THEN 'American'
        WHEN categories LIKE '%Mexican%' THEN 'Mexican'
    END AS restaurant_type,
    stars,
    COUNT(*) AS business_count
FROM yelp.business
WHERE categories IS NOT NULL
  AND (
      categories LIKE '%Chinese%'
      OR categories LIKE '%American%'
      OR categories LIKE '%Mexican%'
  )
GROUP BY
    CASE
        WHEN categories LIKE '%Chinese%' THEN 'Chinese'
        WHEN categories LIKE '%American%' THEN 'American'
        WHEN categories LIKE '%Mexican%' THEN 'Mexican'
    END,
    stars
ORDER BY restaurant_type, stars
""")

restaurant_rating_distribution.show(100, False)