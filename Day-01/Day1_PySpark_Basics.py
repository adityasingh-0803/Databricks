# Day 1: Platform Setup & First Steps
# Databricks + PySpark Basics

# SparkSession is already available in Databricks as `spark`

# -------------------------------
# 1. Create a simple DataFrame
# -------------------------------
data = [
    ("iPhone", 999),
    ("Samsung", 799),
    ("MacBook", 1299),
    ("iPad", 1099)
]

columns = ["product", "price"]

df = spark.createDataFrame(data, columns)

print("Initial DataFrame:")
df.show()

# -------------------------------
# 2. Filter expensive products
# -------------------------------
expensive_products = df.filter(df.price > 1000)

print("Products with price > 1000:")
expensive_products.show()

# -------------------------------
# 3. Select specific columns
# -------------------------------
selected_df = df.select("product")

print("Selected column (product):")
selected_df.show()

# -------------------------------
# 4. Add a new column
# -------------------------------
from pyspark.sql.functions import col

df_with_tax = df.withColumn("price_with_tax", col("price") * 1.18)

print("DataFrame with tax added:")
df_with_tax.show()

# -------------------------------
# 5. Basic aggregation
# -------------------------------
from pyspark.sql.functions import avg, max, min

agg_df = df.agg(
    avg("price").alias("average_price"),
    max("price").alias("max_price"),
    min("price").alias("min_price")
)

print("Aggregated statistics:")
agg_df.show()
