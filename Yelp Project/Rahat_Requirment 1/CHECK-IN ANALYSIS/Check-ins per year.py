%pyspark
result = (
    checkin_exploded
    .groupBy("year")
    .agg(F.count("*").alias("checkin_count"))
    .orderBy("year")
)
print("=== Check-ins Per Year ===")
z.show(result)