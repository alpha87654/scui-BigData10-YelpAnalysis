%pyspark
result = (
    review_df
    .withColumn("day_num",  F.dayofweek(F.col("rev_date")))
    .withColumn("day_name", F.date_format(F.col("rev_date"), "EEEE"))
    .groupBy("day_num", "day_name")
    .agg(
        F.count("review_id").alias("review_count"),
        F.round(F.avg("rev_stars"), 3).alias("avg_rating")
    )
    .orderBy("day_num")
)
print("=== Weekly Rating Frequency ===")
z.show(result)