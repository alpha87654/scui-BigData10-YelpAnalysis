%pyspark
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, count, desc

# Common Chinese menu items to look for
chinese_menu_items = [
    "dumpling","dumplings","dim sum","fried rice","noodles","wonton","wontons",
    "pork bun","bao","ramen","pho","kung pao","mapo","hotpot","hot pot",
    "peking duck","spring roll","spring rolls","egg roll","egg rolls",
    "lo mein","chow mein","bok choy","tofu","congee","scallion pancake",
    "soup dumpling","xiaolongbao","char siu","bbq pork","sesame chicken",
    "orange chicken","mongolian beef","beef broccoli","sweet sour","sticky rice"
]

# Get top 20 Chinese restaurants by review count
top_chinese = business_df.filter(
    col("categories").isNotNull() & col("categories").contains("Chinese")
).orderBy("review_count", ascending=False).limit(20)

print("=== Top 20 Chinese Restaurants ===")
top_chinese.select("name", "city", "state", "review_count", "stars").show(20, truncate=False)
z.show(top_chinese.select("name", "city", "state", "review_count", "stars"))

# Get reviews for these top Chinese restaurants
top_chinese_ids = [row["business_id"] for row in top_chinese.select("business_id").collect()]

chinese_reviews = review_df.filter(col("business_id").isin(top_chinese_ids))

# Extract menu items from reviews
menu_freq = chinese_reviews \
    .select(explode(split(lower(regexp_replace(col("text"), "[^a-zA-Z ]", "")), " ")).alias("word")) \
    .filter(col("word").isin(chinese_menu_items)) \
    .groupBy("word").agg(count("*").alias("mention_count")) \
    .orderBy("mention_count", ascending=False)

print("\n=== Most Mentioned Menu Items in Top Chinese Restaurants ===")
menu_freq.show(20, truncate=False)
z.show(menu_freq)