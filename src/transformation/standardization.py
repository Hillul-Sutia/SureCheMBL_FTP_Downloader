from pyspark.sql.functions import (
    col,
    concat_ws
)

from src.transformation.canonicalization import (
    canonicalize_udf
)

def create_database_id_col(df, id_columns):
    """
    Creates a single Database_ID column.

    If only one ID column exists:
        Database_ID = that column

    If multiple ID columns exist:
        Database_ID = comma-separated values
    """

    if not id_columns:
        raise ValueError(
            "No database ID columns found."
        )

    if len(id_columns) == 1:

        return df.withColumn(
            "Database_ID",
            col(id_columns[0])
        )

    return df.withColumn(
        "Database_ID",
        concat_ws(",", *id_columns)
    )

def add_canonical_data(df, smiles_column):
    """
    Adds canonical_smiles and inchikey columns.
    """

    df = df.withColumn(
        "chemical_data",
        canonicalize_udf(
            col(smiles_column)
        )
    )

    df = (
        df
        .withColumn(
            "canonical_smiles",
            col("chemical_data.canonical_smiles")
        )
        .withColumn(
            "inchikey",
            col("chemical_data.inchikey")
        )
        .drop("chemical_data")
    )

    return df

def standardize_dataset(
    df,
    id_columns,
    smiles_column
):

    df = create_database_id_col(
        df,
        id_columns
    )

    df = add_canonical_data(
        df,
        smiles_column
    )

    return df.select(
        col("Database_ID").alias("database_id"),
        #col(smiles_column).alias("smiles"),
        col("canonical_smiles"),
        col("inchikey")
    )