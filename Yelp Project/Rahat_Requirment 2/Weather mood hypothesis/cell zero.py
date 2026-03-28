%pyspark
business_df = spark.table("default.business")
review_df = spark.table("default.review")
checkin_df = spark.table("default.checkin")

print(business_df.columns)
print(review_df.columns)
print(checkin_df.columns)
