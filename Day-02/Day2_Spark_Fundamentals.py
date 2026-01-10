# Day 2: Apache Spark Fundamentals
# Topics: Spark Architecture, Lazy Evaluation, DataFrame vs RDD

# -----------------------------------
# 1. Read e-commerce CSV data
# -----------------------------------
# CSV uploaded to Databricks path:
# /FileStore/tables/sample_ecommerce.csv

events = spark.read.csv(
    "/FileStore/tables/sample_ecommerce.csv",
    header=True,
    inferSchema=True
)

print("Schema of dataset:")
events.printSchema()

print("Sample records:")
events.show(5)

# -----------------------------------
# 2. Select specific columns
# -----------------------------------
events.select(
    "event_type",
    "product_name",
    "brand",
    "price"
).show(10)

# -----------------------------------
# 3. Filter operation
# -----------------------------------
filtered_count = events.filter(events.price > 100).count()
print("Products with price > 100:", filtered_count)

# -----------------------------------
# 4. GroupBy operation
# -----------------------------------
events.groupBy("event_type").count().show()

# -----------------------------------
# 5. OrderBy (Top brands)
# -----------------------------------
top_brands = (
    events.groupBy("brand")
    .count()
    .orderBy("count", ascending=False)
    .limit(5)
)

top_brands.show()

# -----------------------------------
# 6. Export results
# -----------------------------------
top_brands.write.mode("overwrite").csv(
    "/FileStore/tables/top_brands_output",
    header=True
)

print("Top brands exported successfully.")
