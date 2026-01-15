# Day 7: Databricks Workflows & Job Orchestration
# Topics: Jobs, Parameters, Dependencies, Scheduling

from pyspark.sql import functions as F

# -----------------------------------
# 1. Define Widgets (Parameters)
# -----------------------------------
dbutils.widgets.text("source_path", "/FileStore/tables/sample_ecommerce.csv")
dbutils.widgets.dropdown("layer", "bronze", ["bronze", "silver", "gold"])

source_path = dbutils.widgets.get("source_path")
layer = dbutils.widgets.get("layer")

print(f"Running layer: {layer}")
print(f"Source path: {source_path}")

# -----------------------------------
# 2. Layer Execution Logic
# -----------------------------------
def run_bronze():
    raw = spark.read.csv(source_path, header=True, inferSchema=True)
    bronze = raw.withColumn("ingestion_ts", F.current_timestamp())

    bronze.write.format("delta") \
        .mode("overwrite") \
        .save("/delta/bronze/events")

    print("Bronze layer completed.")

def run_silver():
    bronze = spark.read.format("delta").load("/delta/bronze/events")

    silver = (
        bronze
        .filter(F.col("price") > 0)
        .dropDuplicates(["user_id", "event_type", "product_name"])
    )

    silver.write.format("delta") \
        .mode("overwrite") \
        .save("/delta/silver/events")

    print("Silver layer completed.")

def run_gold():
    silver = spark.read.format("delta").load("/delta/silver/events")

    gold = (
        silver
        .groupBy("product_name")
        .agg(
            F.count("*").alias("events"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("price"))
            ).alias("revenue")
        )
    )

    gold.write.format("delta") \
        .mode("overwrite") \
        .save("/delta/gold/products")

    print("Gold layer completed.")

# -----------------------------------
# 3. Execute based on parameter
# -----------------------------------
if layer == "bronze":
    run_bronze()
elif layer == "silver":
    run_silver()
elif layer == "gold":
    run_gold()
else:
    raise ValueError("Invalid layer selected")
