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

# Create sample DataFrame
data = [
    (1, "Water"),
    (2, "Ethanol"),
    (3, "Acetone")
]

df = spark.createDataFrame(
    data,
    ["compound_id", "compound_name"]
)

print("\n=== Original Data ===")
df.show()

# Write parquet
output_path = "data/parquet/spark_test"

# Remove previous test output if it exists
if os.path.exists(output_path):
    shutil.rmtree(output_path)

print(f"\nWriting parquet to: {output_path}")

df.write.mode("overwrite").parquet(output_path)

print("Parquet write successful")

# Read parquet back
df_read = spark.read.parquet(output_path)

print("\n=== Read Back From Parquet ===")
df_read.show()

print("\n=== Schema ===")
df_read.printSchema()

# Simple validation
original_count = df.count()
read_count = df_read.count()

print(f"\nOriginal rows : {original_count}")
print(f"Read rows     : {read_count}")

if original_count == read_count:
    print("\nSUCCESS: Spark Read/Write Pipeline Working")
else:
    print("\nERROR: Row counts do not match")

spark.stop()

print("\nSpark Session Closed")