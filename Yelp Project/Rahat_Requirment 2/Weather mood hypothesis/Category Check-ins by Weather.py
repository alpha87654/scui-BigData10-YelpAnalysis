%pyspark
warm_cats    = ["Soup", "Hot Pot", "Ramen", "Pho"]
comfort_cats = ["Pizza", "Burgers", "Fast Food", "Sandwiches"]
outdoor_cats = ["Ice Cream", "Food Trucks", "Salad", "Juice Bars"]

tag_warm    = F.udf(lambda c: 1 if c and any(x.lower() in c.lower() for x in warm_cats)    else 0, IntegerType())
tag_comfort = F.udf(lambda c: 1 if c and any(x.lower() in c.lower() for x in comfort_cats) else 0, IntegerType())
tag_outdoor = F.udf(lambda c: 1 if c and any(x.lower() in c.lower() for x in outdoor_cats) else 0, IntegerType())

biz_tagged = (
    philly_biz
    .withColumn("is_warm",    tag_warm(F.col("categories")))
    .withColumn("is_comfort", tag_comfort(F.col("categories")))
    .withColumn("is_outdoor", tag_outdoor(F.col("categories")))
)

checkins_cats = (
    philly_checkins
    .join(biz_tagged.select("business_id","is_warm","is_comfort","is_outdoor"), "business_id")
    .groupBy("checkin_date")
    .agg(
        F.sum("is_warm").alias("warm_checkins"),
        F.sum("is_comfort").alias("comfort_checkins"),
        F.sum("is_outdoor").alias("outdoor_checkins")
    )
)

print("=== Category Check-ins by Weather ===")
z.show(
    checkins_cats
    .join(weather_df, checkins_cats["checkin_date"] == weather_df["weather_date"])
    .groupBy("weather_label")
    .agg(
        F.round(F.avg("warm_checkins"), 1).alias("avg_warm_food"),
        F.round(F.avg("comfort_checkins"), 1).alias("avg_comfort_food"),
        F.round(F.avg("outdoor_checkins"), 1).alias("avg_outdoor_food")
    )
    .orderBy("weather_label")
)