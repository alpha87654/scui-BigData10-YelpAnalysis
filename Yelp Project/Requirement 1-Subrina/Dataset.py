%pyspark
# Reload all datasets after restart
business_df = spark.read.json("hdfs:///user/faria/yelp/business/yelp_academic_dataset_business.json")
review_df   = spark.read.json("hdfs:///user/faria/yelp/review/review.json")
user_df     = spark.read.json("hdfs:///user/faria/yelp/user/user.json")
tip_df      = spark.read.json("hdfs:///user/faria/yelp/tip/tip.json")
checkin_df  = spark.read.json("hdfs:///user/faria/yelp/checkin/checkin.json")

print("✅ business:", business_df.count())
print("✅ review:  ", review_df.count())
print("✅ user:    ", user_df.count())
print("✅ tip:     ", tip_df.count())
print("✅ checkin: ", checkin_df.count())