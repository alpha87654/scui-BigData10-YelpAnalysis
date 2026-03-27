
%pyspark
from pyspark.sql.functions import year, col, count, explode, split, sum as _sum, size, trim

# New users per year
new_users = user_df.withColumn("year", year(col("yelping_since"))) \
    .groupBy("year").agg(count("*").alias("new_users"))

# Reviews per year
reviews_per_year = review_df.withColumn("year", year(col("date"))) \
    .groupBy("year").agg(count("*").alias("reviews"))

# Elite users per year — elite is a comma-separated string like "2012, 2013"
elite_per_year = user_df.filter(col("elite").isNotNull() & (col("elite") != "")) \
    .withColumn("elite_year", explode(split(col("elite"), ", "))) \
    .withColumn("elite_year", trim(col("elite_year")).cast("int")) \
    .filter(col("elite_year").isNotNull()) \
    .groupBy("elite_year").agg(count("*").alias("elite_users")) \
    .withColumnRenamed("elite_year", "year")

# Tips per year
tips_per_year = tip_df.withColumn("year", year(col("date"))) \
    .groupBy("year").agg(count("*").alias("tips"))

# Checkins per year
checkin_per_year = checkin_df \
    .withColumn("dates_array", split(col("date"), ", ")) \
    .withColumn("checkin_count", size(col("dates_array"))) \
    .agg(_sum("checkin_count").alias("total_checkins"))

print("=== New Users Per Year ===")
new_users.orderBy("year").show(30)

print("=== Reviews Per Year ===")
reviews_per_year.orderBy("year").show(30)

print("=== Elite Users Per Year ===")
elite_per_year.orderBy("year").show(30)

print("=== Tips Per Year ===")
tips_per_year.orderBy("year").show(30)

print("=== Total Checkins (all years combined) ===")
checkin_per_year.show()
z.show(checkin_per_year)