from pyspark.sql import SparkSession
import shutil
import os

# Create Spark session
spark = (
    SparkSession.builder
    .appName("SureCheMBL-Spark-Test")
    .master("local[*]")
    .getOrCreate()
)

print("\n=== Spark Session Created Successfully ===")
print("Spark Version:", spark.version)

output_path = "data/processed/latest.parquet"

# Read parquet back
df_read = spark.read.parquet(output_path)

print("\n=== Read Back From Parquet ===")
df_read.show()

print("\n=== Schema ===")
df_read.printSchema()

# Simple validation
read_count = df_read.count()
df_read.show()

print(f"Read rows     : {read_count}")


spark.stop()

print("\nSpark Session Closed")