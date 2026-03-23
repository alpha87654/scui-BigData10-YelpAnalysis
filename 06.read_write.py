from pyspark.sql import SparkSession


spark = (SparkSession.builder
         .appName ("Spark Data Frame Intro")
         .master("local")
         .getOrCreate()
         )
df = spark.read.text('dataset/sparkIntro.txt')

df = spark.read.format("text").load('dataset/sparkIntro.txt')
df.printSchema()
df.show()


df = (spark.read
   .option("header", "True")
      .csv("dataset/BeijingPM20100101_20151231.csv"))

df.write.json("output")

