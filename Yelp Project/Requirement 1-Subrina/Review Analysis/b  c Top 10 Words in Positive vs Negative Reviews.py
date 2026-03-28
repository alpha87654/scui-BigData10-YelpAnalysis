%pyspark
from pyspark.sql.functions import col, explode, split, lower, trim, regexp_replace, count

# Stopwords to filter out
stopwords = set([
    "the","a","an","and","or","but","in","on","at","to","for","of","with",
    "is","was","it","i","my","we","they","this","that","be","are","have",
    "had","not","so","as","if","me","he","she","his","her","their","our",
    "you","your","do","did","its","from","by","but","just","get","got",
    "all","no","can","more","also","very","been","one","out","up","there",
    "what","about","would","when","were","has","than","then","which","who",
    "will","them","some","into","he","she","we","us","him","her","those"
])

# Positive reviews = 4 or 5 stars
positive_words = review_df.filter(col("stars") >= 4) \
    .select(explode(split(lower(regexp_replace(col("text"), "[^a-zA-Z ]", "")), " ")).alias("word")) \
    .filter(col("word") != "") \
    .filter(~col("word").isin(stopwords)) \
    .groupBy("word").agg(count("*").alias("count")) \
    .orderBy("count", ascending=False)

# Negative reviews = 1 or 2 stars
negative_words = review_df.filter(col("stars") <= 2) \
    .select(explode(split(lower(regexp_replace(col("text"), "[^a-zA-Z ]", "")), " ")).alias("word")) \
    .filter(col("word") != "") \
    .filter(~col("word").isin(stopwords)) \
    .groupBy("word").agg(count("*").alias("count")) \
    .orderBy("count", ascending=False)

print("=== Top 10 Words in POSITIVE Reviews (4-5 stars) ===")
positive_words.show(10)
z.show(positive_words)

print("=== Top 10 Words in NEGATIVE Reviews (1-2 stars) ===")
negative_words.show(10)
z.show(negative_words)