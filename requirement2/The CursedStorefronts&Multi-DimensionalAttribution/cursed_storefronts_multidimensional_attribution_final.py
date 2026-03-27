# ============================================
# Yelp Project - Requirement 2
# The Cursed Storefronts & Multi-Dimensional Attribution
# Final version
# ============================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# --------------------------------------------------
# 1. Build restaurant base dataset
# --------------------------------------------------

restaurant_base = spark.sql("""
SELECT
    business_id,
    name,
    address,
    city,
    state,
    postal_code,
    latitude,
    longitude,
    stars,
    review_count,
    is_open,
    categories,
    attributes,
    CONCAT(address, ', ', city, ', ', state, ', ', postal_code) AS full_address
FROM yelp.business
WHERE categories IS NOT NULL
  AND lower(categories) LIKE '%restaurant%'
  AND address IS NOT NULL
  AND trim(address) <> ''
""")

restaurant_base.createOrReplaceTempView("restaurant_base")


# --------------------------------------------------
# 2. Final cursed storefront chart
# --------------------------------------------------

final_cursed_storefronts_chart = spark.sql("""
SELECT
    full_address,
    closed_businesses,
    total_businesses,
    open_businesses,
    closure_ratio,
    avg_stars,
    avg_review_count
FROM (
    SELECT
        full_address,
        COUNT(*) AS total_businesses,
        SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) AS closed_businesses,
        SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) AS open_businesses,
        ROUND(SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS closure_ratio,
        ROUND(AVG(stars), 2) AS avg_stars,
        ROUND(AVG(review_count), 2) AS avg_review_count
    FROM restaurant_base
    GROUP BY full_address
    HAVING COUNT(*) >= 2
       AND SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) >= 2
) t
ORDER BY closed_businesses DESC, closure_ratio DESC, total_businesses DESC, avg_stars ASC
LIMIT 10
""")


# --------------------------------------------------
# 3. Final golden locations chart
# --------------------------------------------------

final_golden_locations_chart = spark.sql("""
SELECT
    full_address,
    open_businesses,
    total_businesses,
    closed_businesses,
    survival_ratio,
    avg_stars,
    avg_review_count
FROM (
    SELECT
        full_address,
        COUNT(*) AS total_businesses,
        SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) AS open_businesses,
        SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) AS closed_businesses,
        ROUND(SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS survival_ratio,
        ROUND(AVG(stars), 2) AS avg_stars,
        ROUND(AVG(review_count), 2) AS avg_review_count
    FROM restaurant_base
    GROUP BY full_address
    HAVING COUNT(*) >= 2
       AND SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) >= 2
) t
ORDER BY open_businesses DESC, survival_ratio DESC, avg_stars DESC, avg_review_count DESC
LIMIT 10
""")


# --------------------------------------------------
# 4. Attribute comparison: cursed vs golden
# --------------------------------------------------

attribute_flag_comparison = spark.sql("""
WITH cursed_top AS (
    SELECT full_address
    FROM (
        SELECT
            full_address,
            COUNT(*) AS total_businesses,
            SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) AS closed_businesses,
            ROUND(SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS closure_ratio
        FROM restaurant_base
        GROUP BY full_address
        HAVING COUNT(*) >= 2
           AND SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) >= 2
        ORDER BY closed_businesses DESC, closure_ratio DESC, total_businesses DESC
        LIMIT 3
    )
),
golden_top AS (
    SELECT full_address
    FROM (
        SELECT
            full_address,
            COUNT(*) AS total_businesses,
            SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) AS open_businesses,
            ROUND(SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS survival_ratio
        FROM restaurant_base
        GROUP BY full_address
        HAVING COUNT(*) >= 2
           AND SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) >= 2
        ORDER BY open_businesses DESC, survival_ratio DESC, total_businesses DESC
        LIMIT 3
    )
),
extreme_businesses AS (
    SELECT
        CASE
            WHEN full_address IN (SELECT full_address FROM cursed_top) THEN 'Cursed'
            WHEN full_address IN (SELECT full_address FROM golden_top) THEN 'Golden'
        END AS location_type,
        full_address,
        is_open,
        stars,
        review_count,
        CAST(attributes AS STRING) AS attr
    FROM restaurant_base
    WHERE full_address IN (SELECT full_address FROM cursed_top)
       OR full_address IN (SELECT full_address FROM golden_top)
)
SELECT
    location_type,
    COUNT(*) AS businesses,
    SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) AS open_count,
    SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) AS closed_count,
    ROUND(AVG(stars), 2) AS avg_stars,
    ROUND(AVG(review_count), 2) AS avg_review_count,
    SUM(CASE WHEN lower(attr) LIKE '%restaurantsdelivery%' AND lower(attr) LIKE '%true%' THEN 1 ELSE 0 END) AS delivery_mentioned_true,
    SUM(CASE WHEN lower(attr) LIKE '%restaurantstakeout%' AND lower(attr) LIKE '%true%' THEN 1 ELSE 0 END) AS takeout_mentioned_true,
    SUM(CASE WHEN lower(attr) LIKE '%restaurantsreservations%' AND lower(attr) LIKE '%true%' THEN 1 ELSE 0 END) AS reservations_true,
    SUM(CASE WHEN lower(attr) LIKE '%outdoorseating%' AND lower(attr) LIKE '%true%' THEN 1 ELSE 0 END) AS outdoor_true,
    SUM(CASE WHEN lower(attr) LIKE '%wifi%' AND lower(attr) LIKE '%no%' THEN 1 ELSE 0 END) AS wifi_no_count,
    SUM(CASE WHEN lower(attr) LIKE '%alcohol%' AND lower(attr) LIKE '%none%' THEN 1 ELSE 0 END) AS alcohol_none_count,
    SUM(CASE WHEN lower(attr) LIKE '%businessparking%' THEN 1 ELSE 0 END) AS parking_info_present
FROM extreme_businesses
GROUP BY location_type
ORDER BY location_type
""")


# --------------------------------------------------
# 5. Review autopsy: low-star recent reviews
# --------------------------------------------------

cursed_low_star_recent_reviews = spark.sql("""
SELECT
    r.full_address,
    r.name,
    rv.review_id,
    rv.rev_stars AS stars,
    rv.rev_date AS review_date,
    SUBSTRING(rv.rev_text, 1, 250) AS review_text_preview
FROM restaurant_base r
JOIN yelp.review rv
  ON r.business_id = rv.rev_business_id
WHERE r.full_address = '160 N Gulph Rd, King of Prussia, PA, 19406'
  AND r.is_open = 0
  AND rv.rev_stars <= 2
ORDER BY rv.rev_date DESC
LIMIT 20
""")


# --------------------------------------------------
# 6. Review autopsy: complaint keyword summary
# --------------------------------------------------

cursed_complaint_keywords = spark.sql("""
WITH base_reviews AS (
    SELECT
        lower(rv.rev_text) AS review_text
    FROM restaurant_base r
    JOIN yelp.review rv
      ON r.business_id = rv.rev_business_id
    WHERE r.full_address = '160 N Gulph Rd, King of Prussia, PA, 19406'
      AND r.is_open = 0
      AND rv.rev_stars <= 2
),
tokenized AS (
    SELECT explode(split(regexp_replace(review_text, '[^a-zA-Z ]', ''), ' ')) AS word
    FROM base_reviews
),
filtered AS (
    SELECT word
    FROM tokenized
    WHERE length(word) >= 4
      AND word NOT IN (
        'this','that','with','they','have','were','from','been','very','just',
        'there','their','would','when','what','your','than','then','them',
        'food','place','really','dont','didnt','will','went','here','because',
        'about','after','only','even','into','while','where','much','more',
        'some','over','again','back','like','made','make','came','which',
        'restaurant'
      )
)
SELECT
    word,
    COUNT(*) AS freq
FROM filtered
GROUP BY word
ORDER BY freq DESC, word ASC
LIMIT 20
""")


# --------------------------------------------------
# 7. External validation summary
# --------------------------------------------------

external_validation_summary = spark.createDataFrame([
    (
        "Cursed",
        "160 N Gulph Rd, King of Prussia, PA, 19406",
        "King of Prussia Mall retail environment",
        "Somewhat walkable / car-oriented area",
        "High tenant competition and mall-based turnover risk"
    ),
    (
        "Golden",
        "51 N 12th St, Philadelphia, PA, 19107",
        "Reading Terminal Market / dense food hub",
        "Very high walkability, strong pedestrian access",
        "Stable destination dining environment with clustered demand"
    )
], [
    "location_type",
    "full_address",
    "external_context",
    "access_signal",
    "business_interpretation"
])


# --------------------------------------------------
# 8. Final cursed vs golden summary
# --------------------------------------------------

final_cursed_vs_golden_summary = spark.sql("""
WITH cursed_base AS (
    SELECT
        full_address,
        COUNT(*) AS total_businesses,
        SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) AS closed_businesses,
        SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) AS open_businesses,
        ROUND(SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS closure_ratio,
        ROUND(AVG(stars), 2) AS avg_stars,
        ROUND(AVG(review_count), 2) AS avg_review_count
    FROM restaurant_base
    GROUP BY full_address
    HAVING COUNT(*) >= 2
       AND SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) >= 2
),
golden_base AS (
    SELECT
        full_address,
        COUNT(*) AS total_businesses,
        SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) AS closed_businesses,
        SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) AS open_businesses,
        ROUND(SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS survival_ratio,
        ROUND(AVG(stars), 2) AS avg_stars,
        ROUND(AVG(review_count), 2) AS avg_review_count
    FROM restaurant_base
    GROUP BY full_address
    HAVING COUNT(*) >= 2
       AND SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) >= 2
)
SELECT
    'Cursed' AS location_type,
    COUNT(*) AS addresses,
    ROUND(AVG(total_businesses), 2) AS avg_total_businesses,
    ROUND(AVG(closed_businesses), 2) AS avg_closed_businesses,
    ROUND(AVG(open_businesses), 2) AS avg_open_businesses,
    ROUND(AVG(closure_ratio), 2) AS avg_extreme_ratio,
    ROUND(AVG(avg_stars), 2) AS avg_stars,
    ROUND(AVG(avg_review_count), 2) AS avg_review_count
FROM (
    SELECT * FROM cursed_base
    ORDER BY closed_businesses DESC, closure_ratio DESC, total_businesses DESC
    LIMIT 10
) c

UNION ALL

SELECT
    'Golden' AS location_type,
    COUNT(*) AS addresses,
    ROUND(AVG(total_businesses), 2) AS avg_total_businesses,
    ROUND(AVG(closed_businesses), 2) AS avg_closed_businesses,
    ROUND(AVG(open_businesses), 2) AS avg_open_businesses,
    ROUND(AVG(survival_ratio), 2) AS avg_extreme_ratio,
    ROUND(AVG(avg_stars), 2) AS avg_stars,
    ROUND(AVG(avg_review_count), 2) AS avg_review_count
FROM (
    SELECT * FROM golden_base
    ORDER BY open_businesses DESC, survival_ratio DESC, avg_stars DESC, avg_review_count DESC
    LIMIT 10
) g
""")


# --------------------------------------------------
# 9. Optional local preview prints
# --------------------------------------------------

if __name__ == "__main__":
    print("restaurant_base sample")
    restaurant_base.show(5, truncate=False)

    print("final_cursed_storefronts_chart")
    final_cursed_storefronts_chart.show(10, truncate=False)

    print("final_golden_locations_chart")
    final_golden_locations_chart.show(10, truncate=False)

    print("attribute_flag_comparison")
    attribute_flag_comparison.show(truncate=False)

    print("cursed_low_star_recent_reviews")
    cursed_low_star_recent_reviews.show(20, truncate=False)

    print("cursed_complaint_keywords")
    cursed_complaint_keywords.show(20, truncate=False)

    print("external_validation_summary")
    external_validation_summary.show(truncate=False)

    print("final_cursed_vs_golden_summary")
    final_cursed_vs_golden_summary.show(truncate=False)