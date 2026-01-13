# Day 5: Delta Lake Advanced
# Topics: MERGE, Time Travel, OPTIMIZE, VACUUM

from delta.tables import DeltaTable

# -----------------------------------
# 1. Load Delta table
# -----------------------------------
delta_table = DeltaTable.forPath(spark, "/delta/events")

# -----------------------------------
# 2. Load incremental updates
# -----------------------------------
updates = spark.read.csv(
    "/FileStore/tables/new_data.csv",
    header=True,
    inferSchema=True
)

# -----------------------------------
# 3. MERGE (Upsert operation)
# -----------------------------------
delta_table.alias("t").merge(
    updates.alias("s"),
    "t.user_id = s.user_id AND t.event_type = s.event_type"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("MERGE operation completed.")

# -----------------------------------
# 4. Time Travel (Version-based)
# -----------------------------------
version_0_df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/delta/events")

print("Delta table at version 0:")
version_0_df.show(5)

# -----------------------------------
# 5. Time Travel (Timestamp-based)
# -----------------------------------
historical_df = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("/delta/events")

print("Historical Delta data:")
historical_df.show(5)

# -----------------------------------
# 6. OPTIMIZE and ZORDER
# -----------------------------------
spark.sql("""
    OPTIMIZE events_table
    ZORDER BY (event_type, user_id)
""")

print("OPTIMIZE completed.")

# -----------------------------------
# 7. VACUUM (cleanup old files)
# -----------------------------------
spark.sql("""
    VACUUM events_table RETAIN 168 HOURS
""")

print("VACUUM completed.")
