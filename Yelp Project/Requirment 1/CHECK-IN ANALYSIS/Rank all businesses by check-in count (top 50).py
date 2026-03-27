%pyspark
result = (
    checkin_exploded
    .groupBy("business_id")
    .agg(F.count("*").alias("total_checkins"))
    .join(
        business_df.select("business_id", "name", "city", "state"),
        "business_id"
    )
    .select("name", "city", "state", "total_checkins")
    .orderBy(F.desc("total_checkins"))
    .limit(50)
)
print("=== Top 50 Businesses by Check-in Count ===")
z.show(result)