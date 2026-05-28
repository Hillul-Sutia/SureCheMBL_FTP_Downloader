from pyspark.sql import SparkSession

def create_spark():

    spark = (
        SparkSession.builder
        .appName("SureChemBL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    return spark