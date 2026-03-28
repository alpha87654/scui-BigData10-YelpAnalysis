
%pyspark
from pyspark.sql import functions as F

CITY  = "Philadelphia"
STATE = "PA"
YEAR  = 2017

business_df = spark.table("default.business")
review_raw  = spark.table("default.review")
checkin_df  = spark.table("default.checkin")

review_df = review_raw.withColumn("rev_date", F.to_date(F.col("rev_timestamp")))

philly_biz = (
    business_df
    .filter(
        (F.col("city") == CITY) &
        (F.col("state") == STATE) &
        F.col("categories").isNotNull() &
        F.lower(F.col("categories")).contains("restaurant")
    )
    .select("business_id", "name", "categories", "stars")
    .cache()
)

philly_reviews = (
    review_df.alias("r")
    .join(
        philly_biz.alias("b"),
        F.col("r.rev_business_id") == F.col("b.business_id"),
        "inner"
    )
    .filter(F.year(F.col("r.rev_date")) == YEAR)
    .select(
        F.col("r.review_id"),
        F.col("r.rev_business_id").alias("business_id"),
        F.col("r.rev_stars"),
        F.col("r.rev_text"),
        F.col("r.rev_date")
    )
    .cache()
)

philly_checkins = (
    checkin_df
    .join(philly_biz.select("business_id"), "business_id", "inner")
    .select(
        "business_id",
        F.explode(F.split(F.col("checkin_dates"), ",")).alias("datetime_raw")
    )
    .withColumn("checkin_dt", F.to_timestamp(F.trim(F.col("datetime_raw"))))
    .withColumn("checkin_date", F.to_date(F.col("checkin_dt")))
    .filter(F.year(F.col("checkin_date")) == YEAR)
    .cache()
)

print(f"Philly restaurant businesses : {philly_biz.count():,}")
print(f"Philly restaurant reviews 2017 : {philly_reviews.count():,}")
print(f"Philly restaurant checkins 2017: {philly_checkins.count():,}")