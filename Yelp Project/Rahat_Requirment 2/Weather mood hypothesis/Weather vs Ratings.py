%pyspark
print("=== Correlation: Weather vs Ratings/Check-ins ===")
print(f"Temperature vs Avg Rating : {reviews_weather.stat.corr('temp_max_c', 'avg_stars'):.4f}")
print(f"Rainfall     vs Avg Rating: {reviews_weather.stat.corr('precip_mm',  'avg_stars'):.4f}")
print(f"Wind Speed   vs Avg Rating: {reviews_weather.stat.corr('wind_speed', 'avg_stars'):.4f}")
print(f"Temperature  vs Check-ins : {checkins_weather.stat.corr('temp_max_c', 'total_checkins'):.4f}")
print(f"Rainfall     vs Check-ins : {checkins_weather.stat.corr('precip_mm',  'total_checkins'):.4f}")
print(
"""
Interpretation guide:
  > +0.3  = moderate positive correlation
  < -0.3  = moderate negative correlation
  ~  0    = no meaningful relationship
""")