%pyspark
from pyspark.sql.functions import col, length, when

# Mixed signal reviews = high stars BUT contains negative words
# OR low stars BUT contains positive words
positive_words_list = ["great","amazing","excellent","delicious","wonderful","fantastic","perfect","best"]
negative_words_list = ["bad","terrible","awful","horrible","disgusting","worst","rude","dirty"]

# Build conditions
pos_condition = None
for w in positive_words_list:
    cond = col("text").contains(w)
    pos_condition = cond if pos_condition is None else pos_condition | cond

neg_condition = None
for w in negative_words_list:
    cond = col("text").contains(w)
    neg_condition = cond if neg_condition is None else neg_condition | cond

# Mixed signal: 4-5 stars BUT has negative words
high_stars_negative = review_df.filter(
    (col("stars") >= 4) & neg_condition
).withColumn("signal_type", when(col("stars") >= 4, "High Stars + Negative Words"))

# Mixed signal: 1-2 stars BUT has positive words
low_stars_positive = review_df.filter(
    (col("stars") <= 2) & pos_condition
).withColumn("signal_type", when(col("stars") <= 2, "Low Stars + Positive Words"))

print("=== Mixed Signal Reviews ===")
print("High stars (4-5) with negative words:", high_stars_negative.count())
print("Low stars (1-2) with positive words:", low_stars_positive.count())

# Show sample mixed signal reviews
mixed_sample = high_stars_negative.select("stars", "signal_type", "text") \
    .union(low_stars_positive.select("stars", "signal_type", "text")) \
    .limit(10)

mixed_sample.show(10, truncate=80)
z.show(mixed_sample)