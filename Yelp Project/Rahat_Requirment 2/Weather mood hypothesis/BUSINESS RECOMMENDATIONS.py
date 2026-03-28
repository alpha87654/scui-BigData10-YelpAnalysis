%pyspark
print("""
=== BUSINESS RECOMMENDATIONS (Philadelphia Restaurants, 2017) ===

1. HEAVY RAIN DAYS
   - Expect slightly lower check-in traffic than normal days
   - Expect a slightly higher 1-star review share
   - Improve wait-time communication, delivery coordination, and service recovery

2. EXTREME COLD DAYS
   - Check-in activity is slightly higher than normal
   - Keep hot meals, hot drinks, and comfort-food inventory ready
   - Maintain solid staffing, especially for indoor dining and takeout

3. STRONG WIND DAYS
   - Expect slightly softer traffic than normal days
   - Reduce reliance on outdoor seating
   - Keep staffing near normal, but avoid overstaffing

4. EXTREME HEAT DAYS
   - Check-ins appear lower, but only 3 extreme-heat days were observed
   - Treat this as a weak signal, not a strong operational rule
   - Consider flexible stock for cold drinks and reduced outdoor seating

5. OVERALL CONCLUSION
   - Weather influences restaurant reputation and traffic modestly, not dramatically
   - Heavy rain has the clearest negative effect on customer satisfaction
   - Cold weather aligns with slightly stronger indoor/comfort-food demand
""")
