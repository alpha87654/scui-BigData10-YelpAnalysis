from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

user_rating_evolution = spark.sql("""
WITH user_reviews AS (
    SELECT
        user_id,
        TO_DATE(date) AS review_date,
        YEAR(TO_DATE(date)) AS review_year,
        stars
    FROM yelp.review
    WHERE user_id IS NOT NULL
      AND date IS NOT NULL
),
first_year_per_user AS (
    SELECT
        user_id,
        MIN(review_year) AS first_year
    FROM user_reviews
    GROUP BY user_id
),
reviews_with_year_offset AS (
    SELECT
        r.user_id,
        r.stars,
        f.first_year,
        r.review_year,
        (r.review_year - f.first_year) AS year_offset
    FROM user_reviews r
    JOIN first_year_per_user f
      ON r.user_id = f.user_id
),
first_vs_third AS (
    SELECT
        user_id,
        AVG(CASE WHEN year_offset = 0 THEN stars END) AS first_year_avg,
        AVG(CASE WHEN year_offset = 2 THEN stars END) AS third_year_avg
    FROM reviews_with_year_offset
    GROUP BY user_id
)
SELECT
    COUNT(*) AS users_with_both_years,
    ROUND(AVG(first_year_avg), 2) AS avg_first_year_rating,
    ROUND(AVG(third_year_avg), 2) AS avg_third_year_rating
FROM first_vs_third
WHERE first_year_avg IS NOT NULL
  AND third_year_avg IS NOT NULL
""")

user_rating_evolution.show(20, False)