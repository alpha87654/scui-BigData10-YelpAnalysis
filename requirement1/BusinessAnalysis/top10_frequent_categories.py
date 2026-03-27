from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

top10_categories = spark.sql("""
SELECT
    TRIM(category) AS category,
    COUNT(*) AS category_count
FROM (
    SELECT EXPLODE(SPLIT(categories, ',')) AS category
    FROM yelp.business
    WHERE categories IS NOT NULL
) t
WHERE TRIM(category) <> ''
GROUP BY TRIM(category)
ORDER BY category_count DESC
LIMIT 10
""")

top10_categories.show(10, False)