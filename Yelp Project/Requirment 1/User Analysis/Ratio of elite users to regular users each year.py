%pyspark
from pyspark.sql.types import ArrayType, StringType

# user_elite is a comma-separated string of years e.g. "2015,2016,2017"
elite_exploded = (
    users_df
    .filter(
        F.col("user_elite").isNotNull() &
        (F.col("user_elite") != "") &
        (F.col("user_elite") != "None")
    )
    .select(
        "user_id",
        F.explode(F.split(F.trim(F.col("user_elite")), ",")).alias("elite_year")
    )
    .withColumn("elite_year", F.trim(F.col("elite_year")).cast("int"))
    .filter(F.col("elite_year").isNotNull())
)

# Count elite users per year
elite_per_year = (
    elite_exploded
    .groupBy("elite_year")
    .agg(F.countDistinct("user_id").alias("elite_count"))
)

# Count all users who joined up to each year (cumulative)
users_per_year = (
    users_df
    .withColumn("join_year", F.year(F.to_date(F.col("user_yelping_since"))))
    .groupBy("join_year")
    .agg(F.count("user_id").alias("total_new_users"))
)

result = (
    elite_per_year
    .join(users_per_year, F.col("elite_year") == F.col("join_year"))
    .withColumn("regular_count", F.col("total_new_users") - F.col("elite_count"))
    .withColumn("elite_ratio_%",
        F.round(F.col("elite_count") / F.col("total_new_users") * 100, 2))
    .select(
        F.col("elite_year").alias("year"),
        "elite_count", "regular_count",
        "total_new_users", "elite_ratio_%"
    )
    .orderBy("year")
)
print("=== Elite vs Regular User Ratio Per Year ===")
z.show(result)