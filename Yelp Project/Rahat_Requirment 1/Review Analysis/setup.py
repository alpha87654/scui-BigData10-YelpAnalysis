%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

business_df = spark.table("default.business")
review_df = spark.table("default.review")
users_df = spark.table("default.users")
checkin_df = spark.table("default.checkin")

print("Tables loaded OK.")
print("Business columns:", business_df.columns)
print("Review columns:  ", review_df.columns)
print("Users columns:   ", users_df.columns)
print("Checkin columns: ", checkin_df.columns)