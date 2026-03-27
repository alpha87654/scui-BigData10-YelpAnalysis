from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL 3.5.3") \
    .getOrCreate()

# Create a DataFrame
df = spark.createDataFrame([('tom', 20), ('jack', 40)], ['name', 'age'])

# Transformation and Action
df.select('name').show()

# Stop the session
spark.stop()