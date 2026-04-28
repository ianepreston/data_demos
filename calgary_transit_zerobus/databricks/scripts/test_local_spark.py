"""Minimal local PySpark smoke test — no Databricks Connect."""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .appName("local-test")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

# Quick sanity check
df = spark.createDataFrame(
    [(1, "alice"), (2, "bob"), (3, "carol")],
    schema=["id", "name"],
)
df.show()
print(f"Row count: {df.count()}")

spark.stop()
print("Spark session created and stopped successfully.")
