# Day 10: Performance Optimization
# Topics: Query plans, Partitioning, ZORDER, Caching

# -----------------------------------
# 1. Analyze Query Execution Plan
# -----------------------------------
spark.sql("""
    SELECT * FROM silver.events
    WHERE event_type = 'purchase'
""").explain(True)

# -----------------------------------
# 2. Create Partitioned Delta Table
# -----------------------------------
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.events_part
    USING DELTA
    PARTITIONED BY (event_date, event_type)
    AS
    SELECT * FROM silver.events
""")

print("Partitioned table created.")

# -----------------------------------
# 3. OPTIMIZE & ZORDER
# -----------------------------------
spark.sql("""
    OPTIMIZE silver.events_part
    ZORDER BY (user_id, product_id)
""")

print("OPTIMIZE with ZORDER completed.")

# -----------------------------------
# 4. Benchmark Query Performance
# -----------------------------------
import time

start = time.time()
spark.sql("""
    SELECT * FROM silver.events
    WHERE user_id = 12345
""").count()
print(f"Query time before cache: {time.time() - start:.2f}s")

# -----------------------------------
# 5. Cache Table for Iterative Queries
# -----------------------------------
cached_df = spark.table("silver.events").cache()
cached_df.count()  # Materialize cache

start = time.time()
cached_df.filter("user_id = 12345").count()
print(f"Query time after cache: {time.time() - start:.2f}s")
