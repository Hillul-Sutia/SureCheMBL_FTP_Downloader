# test_spark.py

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

df = spark.createDataFrame([
    (1, "hello"),
    (2, "world")
], ["id", "text"])

df.show()

spark.stop()