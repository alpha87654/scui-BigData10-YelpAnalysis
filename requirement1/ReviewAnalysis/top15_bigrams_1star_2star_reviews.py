from pyspark.sql.functions import col, lower, regexp_replace, split, expr, explode, count, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

clean_reviews = spark.sql("""
SELECT
    stars,
    lower(regexp_replace(text, '[^a-zA-Z\\s]', ' ')) AS clean_text
FROM yelp.review
WHERE stars IN (1, 2)
  AND text IS NOT NULL
LIMIT 50000
""")

words_df = clean_reviews.select(
    col("stars"),
    split(col("clean_text"), "\\s+").alias("words")
)

filtered_words_df = words_df.select(
    col("stars"),
    expr("""
        filter(
            words,
            x -> x != '' AND
                 length(x) > 2 AND
                 x NOT IN (
                    'the','and','for','was','are','but','not','you','all','had','have',
                    'this','that','with','they','she','him','her','his','our','out',
                    'who','what','when','where','why','how','too','very','can','could',
                    'would','should','into','from','there','their','them','then','than',
                    'get','got','just','about','because','been','being','did','didn',
                    'don','does','doesn','wasn','weren','isn','aren','has','hasn',
                    'hadn','will','won','wouldn','couldn','shouldn','one','two','three',
                    'place','really','also'
                 )
        )
    """).alias("filtered_words")
)

bigrams_df = filtered_words_df.select(
    col("stars"),
    expr("""
        transform(
            sequence(1, size(filtered_words) - 1),
            i -> concat(filtered_words[i - 1], ' ', filtered_words[i])
        )
    """).alias("bigrams")
)

bigram_counts = (
    bigrams_df
    .select(col("stars"), explode(col("bigrams")).alias("bigram"))
    .groupBy("stars", "bigram")
    .agg(count("*").alias("bigram_count"))
)

window_spec = Window.partitionBy("stars").orderBy(col("bigram_count").desc())

top15_bigrams = (
    bigram_counts
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") <= 15)
    .orderBy("stars", "rank")
)

top15_bigrams.show(50, False)