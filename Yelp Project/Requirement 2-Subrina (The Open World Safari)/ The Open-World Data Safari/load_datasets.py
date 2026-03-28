%pyspark
def load_datasets(spark):
    business_df = spark.read.json("hdfs:///user/faria/yelp/business/...")
    review_df   = spark.read.json("hdfs:///user/faria/yelp/review/...")
    print("✅ business:", business_df.count())
    print("✅ review:  ", review_df.count())
    return business_df, review_df