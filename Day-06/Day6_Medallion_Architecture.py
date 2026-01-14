# Day 6: Medallion Architecture
# Bronze -> Silver -> Gold pipeline using Delta Lake

from pyspark.sql import functions as F

# -----------------------------------
# BRONZE LAYER: Raw ingestion
# -----------------------------------
raw = spark.read.csv(
    "/FileStore/tables/sample_ecommerce.csv",
    header=True,
    inferSchema=True
)

bronze = raw.withColumn(
    "ingestion_ts",
    F.current_timestamp()
)

bronze.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/bronze/events")

print("Bronze layer created.")

# -----------------------------------
# SILVER LAYER: Cleaning & validation
# -----------------------------------
bronze_df = spark.read.format("delta").load("/delta/bronze/events")

silver = (
    bronze_df
    .filter(F.col("price") > 0)
    .filter(F.col("price") < 10000)
    .dropDuplicates(["user_id", "event_type", "product_name"])
    .withColumn("event_date", F.current_date())
    .withColumn(
        "price_tier",
        F.when(F.col("price") < 500, "budget")
         .when(F.col("price") < 1000, "mid")
         .otherwise("premium")
    )
)

silver.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/silver/events")

print("Silver layer created.")

# -----------------------------------
# GOLD LAYER: Business aggregates
# -----------------------------------
silver_df = spark.read.format("delta").load("/delta/silver/events")

product_perf = (
    silver_df
    .groupBy("product_name", "brand")
    .agg(
        F.countDistinct(
            F.when(F.col("event_type") == "view", F.col("user_id"))
        ).alias("views"),
        F.countDistinct(
            F.when(F.col("event_type") == "purchase", F.col("user_id"))
        ).alias("purchases"),
        F.sum(
            F.when(F.col("event_type") == "purchase", F.col("price"))
        ).alias("revenue")
    )
    .withColumn(
        "conversion_rate",
        (F.col("purchases") / F.col("views")) * 100
    )
)

product_perf.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/gold/products")

print("Gold layer created.")
