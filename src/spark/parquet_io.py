from pyspark.sql import DataFrame

def read_parquet(spark, path: str) -> DataFrame:
    return spark.read.parquet(path)


def write_parquet(df, path: str):
    df.write.mode("overwrite").parquet(path)