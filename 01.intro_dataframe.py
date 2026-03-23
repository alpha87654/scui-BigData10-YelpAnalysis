from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Spark Data Frame Intro")
         .master("local")
         .getOrCreate()
         )

data = [("tom", 2), ("jerry", 1)]
df = spark.createDataFrame(data, ["name", "age"])

df.printSchema()
df.show()
spark.stop()