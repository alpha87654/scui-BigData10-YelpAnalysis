# 1. Open a session to spark
from pyspark.sql import SparkSession

spark=(SparkSession.builder
 .appName("Spark Data Frame Intro")
 .master("local")
 .getOrCreate()
 )
# 2. DO bigdata analysis
data = [('tom', 20), ('jack', 18)]
df = spark.createDataFrame(data, ['name', 'age'])

df.printSchema()  # Display schema
df.show()  # Display data
# 3. close the session
spark.stop()
