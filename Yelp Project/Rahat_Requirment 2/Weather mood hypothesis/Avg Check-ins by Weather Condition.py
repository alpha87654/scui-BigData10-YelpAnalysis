%pyspark
daily_checkins = (
    philly_checkins
    .groupBy("checkin_date")
    .agg(F.count("*").alias("total_checkins"))
)

checkins_weather = (
    daily_checkins
    .join(weather_df, daily_checkins["checkin_date"] == weather_df["weather_date"])
)

print("=== Avg Check-ins by Weather Condition ===")
z.show(
    checkins_weather
    .groupBy("weather_label")
    .agg(
        F.count("checkin_date").alias("days"),
        F.round(F.avg("total_checkins"), 1).alias("avg_daily_checkins"),
        F.round(F.stddev("total_checkins"), 1).alias("stddev_checkins")
    )
    .orderBy(F.desc("avg_daily_checkins"))
)
