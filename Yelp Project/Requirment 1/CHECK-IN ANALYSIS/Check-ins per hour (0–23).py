%pyspark
result = (
    checkin_exploded
    .groupBy("hour")
    .agg(F.count("*").alias("checkin_count"))
    .orderBy("hour")
)
print("=== Check-ins Per Hour ===")
z.show(result)
