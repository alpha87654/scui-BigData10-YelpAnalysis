%pyspark
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, count

# Simple POS-like grouping using word lists instead of NLTK
# Adjectives commonly found in reviews
adjectives = [
    "good","great","bad","excellent","terrible","amazing","awful","wonderful",
    "horrible","fantastic","poor","delicious","fresh","cold","hot","slow","fast",
    "friendly","rude","clean","dirty","cheap","expensive","nice","lovely",
    "disgusting","tasty","bland","spicy","crispy","soggy","overpriced","crowded",
    "quiet","loud","cozy","comfortable","small","large","busy","empty","perfect",
    "mediocre","disappointing","impressive","authentic","average","decent","best","worst"
]

# Nouns commonly found in reviews
nouns = [
    "food","service","place","staff","restaurant","order","time","menu","price",
    "chicken","pizza","burger","sushi","steak","pasta","salad","coffee","beer",
    "wine","sauce","cheese","bread","meat","fish","rice","soup","dessert",
    "waiter","manager","owner","table","seat","parking","location","atmosphere",
    "experience","quality","portion","flavor","taste","value","wait","reservation"
]

all_pos_words = adjectives + nouns

# Sample reviews for word cloud
sample_reviews = review_df.sample(fraction=0.1, seed=42)

# Extract and count POS words
word_freq = sample_reviews \
    .select(explode(split(lower(regexp_replace(col("text"), "[^a-zA-Z ]", "")), " ")).alias("word")) \
    .filter(col("word").isin(all_pos_words)) \
    .groupBy("word").agg(count("*").alias("frequency")) \
    .orderBy("frequency", ascending=False) \
    .limit(30)

print("=== Top 30 Nouns & Adjectives (Word Cloud Data) ===")
word_freq.show(30, truncate=False)
z.show(word_freq)