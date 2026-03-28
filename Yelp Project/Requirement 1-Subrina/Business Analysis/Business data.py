%pyspark
# Load business data
business_df = spark.read.json("hdfs:///user/faria/yelp/business/yelp_academic_dataset_business.json")

print("Total businesses:", business_df.count())
business_df.printSchema()