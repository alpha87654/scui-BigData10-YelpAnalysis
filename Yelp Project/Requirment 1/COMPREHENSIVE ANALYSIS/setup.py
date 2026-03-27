%pyspark
# checkin_dates is a comma-separated string of datetimes
checkin_exploded = (
    checkin_df
    .select(
        "business_id",
        F.explode(F.split(F.col("checkin_dates"), ",")).alias("datetime_raw")
    )
    .withColumn("datetime", F.to_timestamp(F.trim(F.col("datetime_raw"))))
    .withColumn("year",  F.year(F.col("datetime")))
    .withColumn("month", F.month(F.col("datetime")))
    .withColumn("hour",  F.hour(F.col("datetime")))
    .cache()
)
print(f"Total check-in records: {checkin_exploded.count():,}")