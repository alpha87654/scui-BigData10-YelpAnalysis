from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Spark Data Frame Intro")
         .master("local")
         .getOrCreate()
         )

#data = [("tom", 2), ("jerry", 1)]
#df = spark.createDataFrame(data, ["name", "age"])

df = (spark.read
   .option("header", "True")
      .csv("dataset/BeijingPM20100101_20151231.csv"))
df.printSchema()
df.show()

df.printSchema()

df.show()

selected_df = df.select("year" , "month" , "PM_Dongsi")
selected_df.printSchema()
selected_df.show()

result_df = (selected_df
             .where("PM_Dongsi != 'NA'")
             .groupby("year")
             .count())

result_df.show()

spark.stop()