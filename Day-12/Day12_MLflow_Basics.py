# Day 12: MLflow Basics
# Topics: Experiment tracking, model logging, run comparison

import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# -----------------------------------
# 1. Prepare data
# -----------------------------------
df = spark.table("gold.products").toPandas()

X = df[["views", "cart_adds"]] if "cart_adds" in df.columns else df[["views"]]
y = df["purchases"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# -----------------------------------
# 2. MLflow Experiment
# -----------------------------------
mlflow.set_experiment("Databricks_MLflow_Basics")

with mlflow.start_run(run_name="linear_regression_v1"):

    # Log parameters
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("test_size", 0.2)

    # Train model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Evaluate model
    score = model.score(X_test, y_test)
    mlflow.log_metric("r2_score", score)

    # Log model
    mlflow.sklearn.log_model(model, artifact_path="model")

    print(f"R² Score: {score:.4f}")

print("MLflow run completed.")
