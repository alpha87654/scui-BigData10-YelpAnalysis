%pyspark
from pyspark.sql.functions import col

# Read as text and parse correctly
census_text = spark.sparkContext.textFile("hdfs:///user/faria/yelp/external/census_income.json")

# Skip header, parse each line
census_parsed = census_text \
    .filter(lambda line: "ZCTA5" in line) \
    .map(lambda line: line.strip().rstrip(',').strip('[]').replace('"', '').split(',')) \
    .filter(lambda parts: len(parts) == 3) \
    .map(lambda parts: (parts[2].strip(), parts[1].strip()))

# Convert to DataFrame
census_df = census_parsed.toDF(["zip_code", "median_income"])

# Convert income to integer and filter out invalid values
census_df = census_df \
    .withColumn("median_income", col("median_income").cast("integer")) \
    .filter(col("median_income") > 0)

print("✅ Census records:", census_df.count())
print("\nSample Census data:")
census_df.show(10, truncate=False)
census_df.orderBy("median_income", ascending=False).show(10)