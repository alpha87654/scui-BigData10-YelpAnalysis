%pyspark
biz_cats = (
    business_df
    .filter(F.col("categories").isNotNull())
    .select(
        "business_id",
        F.explode(F.split(F.col("categories"), ", ")).alias("category")
    )
    .select("business_id", F.trim(F.col("category")).alias("category"))
)

result = (
    biz_cats.alias("a")
    .join(
        biz_cats.alias("b"),
        (F.col("a.business_id") == F.col("b.business_id")) &
        (F.col("a.category") < F.col("b.category"))
    )
    .select(
        F.col("a.category").alias("category_1"),
        F.col("b.category").alias("category_2")
    )
    .groupBy("category_1", "category_2")
    .agg(F.count("*").alias("co_occurrence_count"))
    .orderBy(F.desc("co_occurrence_count"))
    .limit(10)
)
print("=== Top 10 Category Synergy Pairs ===")
z.show(result)