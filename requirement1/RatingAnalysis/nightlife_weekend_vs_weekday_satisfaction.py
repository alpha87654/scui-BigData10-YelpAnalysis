from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

nightlife_weekend_weekday = spark.sql("""
SELECT
    CASE
        WHEN DAYOFWEEK(TO_TIMESTAMP(r.date)) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    ROUND(AVG(r.stars), 2) AS average_rating,
    COUNT(*) AS review_count
FROM yelp.review r
JOIN yelp.business b
  ON r.business_id = b.business_id
WHERE b.categories LIKE '%Nightlife%'
  AND r.date IS NOT NULL
GROUP BY
    CASE
        WHEN DAYOFWEEK(TO_TIMESTAMP(r.date)) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END
ORDER BY day_type
""")

nightlife_weekend_weekday.show(10, False)