from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

spark = (SparkSession.builder
         .appName("Spark Data Frame Intro")
         .master("local")
         .getOrCreate()
         )

df = (spark.read
      .option("header", "True")
      .option("inferSchema", "True")
      .csv("dataset/BeijingPM20100101_20151231.csv"))

schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("First", StringType(), True),
    StructField("Last", StringType(), True),
    StructField("Url", StringType(), True),
    StructField("Published", StringType(), True),
    StructField("Campaigns", ArrayType(StringType()), True),
])

df = (spark.read
 .schema(schema)
 .json("dataset/blogs.txt"))

df.printSchema()
df.show()

spark.stop()