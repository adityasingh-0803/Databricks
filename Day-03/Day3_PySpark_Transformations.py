# Day 3: PySpark Transformations Deep Dive
# Topics: Joins, Window Functions, UDFs, Feature Engineering

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------
# 1. Load e-commerce dataset
# -----------------------------------
events = spark.read.csv(
    "/FileStore/tables/sample_ecommerce.csv",
    header=True,
    inferSchema=True
)

# -----------------------------------
# 2. Create additional reference DataFrame (products)
# -----------------------------------
products_data = [
    (101, "iPhone 14", "electronics"),
    (102, "Galaxy S23", "electronics"),
    (103, "MacBook Pro", "electronics"),
    (104, "Apple Watch", "wearables"),
    (105, "iPad", "electronics")
]

products = spark.createDataFrame(
    products_data,
    ["product_id", "product_name", "category_code"]
)

# -----------------------------------
# 3. Perform Join
# -----------------------------------
joined_df = events.join(
    products,
    on="product_name",
    how="inner"
)

joined_df.show(5)

# -----------------------------------
# 4. Top 5 products by revenue
# -----------------------------------
revenue = (
    joined_df
    .filter(F.col("event_type") == "purchase")
    .groupBy("product_id", "product_name")
    .agg(F.sum("price").alias("revenue"))
    .orderBy(F.desc("revenue"))
    .limit(5)
)

revenue.show()

# -----------------------------------
# 5. Window function: running total per user
# -----------------------------------
window_spec = Window.partitionBy("user_id").orderBy("event_time")

events_with_running_total = events.withColumn(
    "cumulative_events",
    F.count("*").over(window_spec)
)

events_with_running_total.show(5)

# -----------------------------------
# 6. Conversion rate by category
# -----------------------------------
conversion = (
    joined_df
    .groupBy("category_code", "event_type")
    .count()
    .pivot("event_type")
    .sum("count")
    .withColumn(
        "conversion_rate",
        (F.col("purchase") / F.col("view")) * 100
    )
)

conversion.show()

# -----------------------------------
# 7. UDF: Price category
# -----------------------------------
def price_bucket(price):
    if price < 500:
        return "Low"
    elif price < 1000:
        return "Medium"
    else:
        return "High"

price_udf = F.udf(price_bucket)

final_df = joined_df.withColumn(
    "price_category",
    price_udf(F.col("price"))
)

final_df.show(5)
