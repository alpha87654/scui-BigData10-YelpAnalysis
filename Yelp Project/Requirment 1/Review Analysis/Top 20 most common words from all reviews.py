%pyspark
from pyspark.sql.types import ArrayType, StringType

stopwords = set([
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of",
    "is", "it", "was", "i", "my", "we", "this", "that", "with", "they", "their",
    "have", "had", "has", "be", "are", "were", "as", "not", "so", "if", "by",
    "he", "she", "his", "her", "you", "your", "from", "our", "there", "been",
    "would", "will", "one", "all", "just", "also", "about", "which", "when",
    "up", "out", "do", "did", "its", "get", "got", "more", "very", "no", "can",
    "me", "us", "than", "then", "too", "what", "who", "how", "some", "time",
    "even", "back", "went", "place", "good", "great", "food", "restaurant"
])


def clean_words(text):
    if text is None:
        return []
    result = []
    for w in text.lower().split():
        w = ''.join(c for c in w if c.isalpha())
        if len(w) > 2 and w not in stopwords:
            result.append(w)
    return result


clean_udf = F.udf(clean_words, ArrayType(StringType()))

result = (
    review_df
    .select(clean_udf(F.col("rev_text")).alias("words"))
    .select(F.explode("words").alias("word"))
    .groupBy("word")
    .agg(F.count("*").alias("word_count"))
    .orderBy(F.desc("word_count"))
    .limit(20)
)
print("=== Top 20 Most Common Words in All Reviews ===")
z.show(result)