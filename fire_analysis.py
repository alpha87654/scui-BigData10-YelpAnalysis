from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, countDistinct, to_timestamp, sum, avg, min, max, year, month, weekofyear
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, FloatType

spark = (SparkSession.builder
         .appName ("Spark Data Frame Intro")
         .master("local")
         .getOrCreate()
         )

fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                         StructField('UnitID', StringType(), True),
                         StructField('IncidentNumber', IntegerType(), True),
                         StructField('CallType', StringType(), True),
                         StructField('CallDate', StringType(), True),
                         StructField('WatchDate', StringType(), True),
                         StructField('CallFinalDisposition', StringType(), True),
                         StructField('AvailableDtTm', StringType(), True),
                         StructField('Address', StringType(), True),
                         StructField('City', StringType(), True),
                         StructField('Zipcode', IntegerType(), True),
                         StructField('Battalion', StringType(), True),
                         StructField('StationArea', StringType(), True),
                         StructField('Box', StringType(), True),
                         StructField('OriginalPriority', StringType(), True),
                         StructField('Priority', StringType(), True),
                         StructField('FinalPriority', IntegerType(), True),
                         StructField('ALSUnit', BooleanType(), True),
                         StructField('CallTypeGroup', StringType(), True),
                         StructField('NumAlarms', IntegerType(), True),
                         StructField('UnitType', StringType(), True),
                         StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                         StructField('FirePreventionDistrict', StringType(), True),
                         StructField('SupervisorDistrict', StringType(), True),
                         StructField('Neighborhood', StringType(), True),
                         StructField('Location', StringType(), True),
                         StructField('RowID', StringType(), True),
                         StructField('Delay', FloatType(), True)])

fire_df = spark.read.schema(fire_schema).csv("dataset/sf-fire-calls.txt")

fire_df.printSchema()
fire_df.show(5)

fire_df.write.saveAsTable("fire_calls_table")
spark.sql("SELECT * FROM fire_calls_table LIMIT 5").show()
