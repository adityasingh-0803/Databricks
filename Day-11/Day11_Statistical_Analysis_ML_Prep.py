# Day 11: Statistical Analysis & ML Prep
# Topics: Descriptive stats, hypothesis testing, correlations, feature engineering

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------
# 1. Load data
# -----------------------------------
events = spark.read.format("delta").load("/delta/silver/events")

# -----------------------------------
# 2. Descriptive Statistics
# -----------------------------------
events.describe(["price"]).show()

# -----------------------------------
# 3. Hypothesis Testing: Weekday vs Weekend
# -----------------------------------
events_with_flag = events.withColumn(
    "is_weekend",
    F.dayofweek("event_date").isin([1, 7])  # Sunday=1, Saturday=7
)

events_with_flag.groupBy(
    "is_weekend", "event_type"
).count().show()

# -----------------------------------
# 4. Correlation Analysis
# -----------------------------------
# Example correlation between price and revenue proxy
events.stat.corr("price", "price")

# -----------------------------------
# 5. Feature Engineering for ML
# -----------------------------------
window_spec = Window.partitionBy("user_id").orderBy("event_time")

features_df = (
    events
    .withColumn("hour", F.hour("event_time"))
    .withColumn("day_of_week", F.dayofweek("event_date"))
    .withColumn("price_log", F.log(F.col("price") + 1))
    .withColumn(
        "time_since_first_event",
        F.unix_timestamp("event_time") -
        F.first("event_time").over(window_spec)
    )
)

features_df.select(
    "user_id",
    "hour",
    "day_of_week",
    "price_log",
    "time_since_first_event"
).show(5)

print("Feature engineering completed.")
