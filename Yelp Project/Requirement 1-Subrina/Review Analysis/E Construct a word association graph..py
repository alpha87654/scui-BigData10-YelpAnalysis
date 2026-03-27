%pyspark
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, count

# Key words to find associations for
key_words = ["food","service","price","staff","location","wait",
             "chicken","pizza","burger","coffee","sushi","pasta"]

# Get word pairs that appear in the same review
word_pairs = review_df.sample(fraction=0.02, seed=42) \
    .select("review_id",
            explode(split(lower(regexp_replace(col("text"), "[^a-zA-Z ]", "")), " ")).alias("word")) \
    .filter(col("word").isin(key_words))

# Self join to get co-occurring pairs
pairs = word_pairs.alias("a").join(
    word_pairs.alias("b"),
    (col("a.review_id") == col("b.review_id")) & (col("a.word") < col("b.word"))
).select(
    col("a.word").alias("word1"),
    col("b.word").alias("word2")
).groupBy("word1", "word2") \
 .agg(count("*").alias("association_count")) \
 .orderBy("association_count", ascending=False) \
 .limit(20)

print("=== Top 20 Word Associations ===")
pairs.show(20, truncate=False)
z.show(pairs)