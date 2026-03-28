%pyspark
weather_df = (
    weather_raw
    .select(
        F.to_date(F.col("DATE")).alias("weather_date"),
        # TMAX/TMIN are in °F → convert to °C
        F.round((F.col("TMAX").cast(FloatType()) - 32) * 5/9, 1).alias("temp_max_c"),
        F.round((F.col("TMIN").cast(FloatType()) - 32) * 5/9, 1).alias("temp_min_c"),
        # PRCP is in inches → convert to mm
        F.round(F.col("PRCP").cast(FloatType()) * 25.4, 2).alias("precip_mm"),
        F.col("AWND").cast(FloatType()).alias("wind_speed")
    )
    .filter(F.year(F.col("weather_date")) == YEAR)
    .withColumn("is_heavy_rain",   F.when(F.col("precip_mm") >= 10, 1).otherwise(0))
    .withColumn("is_extreme_heat", F.when(F.col("temp_max_c") >= 35, 1).otherwise(0))
    .withColumn("is_extreme_cold", F.when(F.col("temp_min_c") <= -5, 1).otherwise(0))
    .withColumn("is_strong_wind",  F.when(F.col("wind_speed") >= 10, 1).otherwise(0))
    .withColumn(
        "weather_label",
        F.when(F.col("is_heavy_rain")   == 1, "Heavy Rain")
         .when(F.col("is_extreme_heat") == 1, "Extreme Heat")
         .when(F.col("is_extreme_cold") == 1, "Extreme Cold")
         .when(F.col("is_strong_wind")  == 1, "Strong Wind")
         .otherwise("Normal")
    )
)

print("=== Weather Sample ===")
weather_df.show(5)

print("=== Weather Label Distribution ===")
weather_df.groupBy("weather_label").count().orderBy(F.desc("count")).show()